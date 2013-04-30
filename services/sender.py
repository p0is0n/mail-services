# -*- coding: UTF-8 -*-
# Copyright 2013 p0is0n (poisonoff@gmail.com).
#
# This file is part of Mail-Services.
#
# Mail-Services is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Mail-Services is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Mail-Services.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
import random
import re
import hashlib

from itertools import imap, chain
from cStringIO import StringIO
from pprint import pprint
from time import strftime, time
from uuid import uuid4
from types import ListType, TupleType, UnicodeType, DictType
from json import loads, dumps
from collections import defaultdict

from OpenSSL.SSL import SSLv3_METHOD

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.header import Header
from email.message import Message
from smtplib import SMTP_PORT, SMTP_SSL_PORT

from twisted.mail import relaymanager
from twisted.internet.ssl import ClientContextFactory

from twisted.python.log import msg, err
from twisted.internet.defer import Deferred, DeferredList, DeferredLock, DeferredSemaphore
from twisted.internet.defer import DeferredQueue, inlineCallbacks, returnValue
from twisted.internet.defer import fail, succeed
from twisted.internet.task import cooperate
from twisted.internet.reactor import callLater
from twisted.internet import reactor
from twisted.web.http import OK, NOT_FOUND, PARTIAL_CONTENT
from twisted.application.service import Service

from core.db import sqlite
from core.dirs import tmp
from core.constants import DEBUG, DEBUG_SENDER, CHARSET
from core.utils import sleep
from core.configs import config
from core.smtp import ESMTPSenderFactory
from core.http import Headers, HttpAgent, BufferProtocol, FileProtocol, ContentDecoderAgent, GzipDecoder, HTTPError


class SenderService(Service):

	queueWorkers = 1
	queueInterval = 1
	queueRunning = False
	queueStop = object()
	queueLimit = 2

	# DO NOT EDIT!
	semaphore = DeferredLock()

	sleepValue = 60
	sleepSplit = 5

	def __init__(self):
		self.name = 'Sender'
		self.loop = -1
		self.queue = None

		# Inside
		self._stopCall = None
		self._stopDeferred = None

		self._workers = 0
		self._process = 0
		self._fetcher = 0

		self._state = 'stopped'

		# Http agent
		self._pool = HttpAgent(reactor)
		self._agent = ContentDecoderAgent(self._pool, (('gzip', GzipDecoder), ))

	@property
	def isStarted(self):
		return self.loop != -1 and self._state == 'started'

	@inlineCallbacks
	def sleepWithFireOnServiceStop(self, timeout, split):
		# Sleep
		for i in xrange(split, timeout + split, split):
			if self.loop == -1:
				returnValue(None)

			yield sleep(split)

	@inlineCallbacks
	def sleepOneWithFireOnServiceStop(self, timeout):
		# Sleep
		for i in xrange(1, timeout + 1):
			if not self.isStarted:
				returnValue(None)

			yield sleep(1)

	def startService(self):
		if self.loop != -1 or self._state == 'started':
			msg(self.name, 'start fail - already started', system='-')

			# Already started
			return

		# State
		self._state = 'starting'

		# Show info
		msg(self.name, 'start', system='-')

		# Cancel stop
		if self._stopCall:
			self._stopCall.cancel()
			self._stopCall = None

			# Call stop
			self._stopDeferred.callback(0)
			self._stopDeferred = None

		if self.queue is None:
			self.queue = DeferredQueue()

		# Start generator
		cooperate(self.queueGenerator())

		Service.startService(self)

		self.queueRunning = True
		self.loop = 0

		self._state = 'started'

		# Show info
		msg(self.name, 'started', system='-')

	def stopService(self):
		deferred = Deferred()

		if self.loop == -1 or self._state == 'stopped':
			msg(self.name, 'stops fail - already stopped', system='-')

			# Already stopped
			return

		# State
		self._state = 'stopping'
		self.loop = -1

		# Show info
		msg(self.name, 'stops')

		def s1(code, self=self):
			(msg(self.name,
				'alive workers', self._workers,
				'alive process', self._process,
				'alive fetcher', self._fetcher,
			))

			# Try stop workers
			if self._workers > 0:
				for worker in xrange(1, self._workers + 1):
					self.queue.put(self.queueStop)

			if (self._process + self._workers + self._fetcher) > 0:
				# Fail, wait...
				self._stopCall = callLater(1, s1, 0)
			else:
				self._stopCall = callLater(0, deferred.callback, 1)

		self._stopCall = callLater(1, s1, 0)
		self._stopDeferred = deferred

		def s2(code, self=self):
			try:
				if code != 1:
					# Cancel stop
					return

				if not (self._state == 'stopping' and (self._process + self._workers) == 0):
					err(RuntimeError('{0} stop error: state-{1} p{2} w{3}'.format(
						self.name,
						self._state,
						self._process,
						self._workers,
					)))

				self._stopCall = None
				self._stopDeferred = None

				# Inside
				Service.stopService(self)

				self.queue = None
				self._state = 'stopped'

				# Show info
				msg(self.name, 'stopped', system='-')
			except:
				err()

		return deferred.addCallback(s2)

	senderIntervalEmpty = config.getfloat('sender', 'interval-empty')
	senderIntervalNext = config.getfloat('sender', 'interval-next')

	def queueGenerator(self):
		self._fetcher += 1

		try:
			msg(self.name, 'start queueGenerator', system='-')

			while self.isStarted:
				try:

					# Start workers
					for i in xrange(self._workers + 1, self.queueWorkers + 1):
						yield cooperate(self.queueWorker(i))

					@inlineCallbacks
					def _c(timeout=1, self=self):
						rows = (yield sqlite.runQuery(
							"SELECT rowid, message_id, group_id, email, name, parts FROM queue_to ORDER BY priority, rowid LIMIT 0, {0}".format(self.queueWorkers * 5)
						))

						if not rows:
							# Sleep
							yield sleep(self.senderIntervalEmpty)
						else:
							queues = []
							delete = []

							for row in rows:
								rowId = row[0]
								rowMessage = row[1]
								rowGroup = row[2]
								rowTime = None

								try:
									# Get message
									message = (yield sqlite.runQuery(
										"SELECT subject, text, html, params FROM queue_messages WHERE rowid = ? LIMIT 1",
										(rowMessage, )
									))

									if not message:
										# Fallback
										raise RuntimeError('Message {0} not found'.format(rowMessage))
									else:
										message = (dict(
											id=rowMessage,
											subject=message[0][0],
											text=message[0][1],
											html=message[0][2],
											params=loads(message[0][3])
										))

									row = (dict(
										id=rowId,
										message=message,
										group=row[2] if row[2] else None,
										email=row[3],
										name=row[4],
										parts=loads(row[5]),
									))
								except Exception, e:
									traceback = sys.exc_info()[2]

									if not traceback:
										traceback = None

									if rowGroup:
										# Update group counters
										(yield sqlite.runOperation(
											"UPDATE queue_groups SET wait = (wait - 1), errors = (errors + 1) WHERE rowid = ?",
											(rowGroup, )
										))

									# Throw
									raise e, None, traceback

								queues.append(row)
								delete.append(str(rowId))

							# Remove from queue
							(yield sqlite.runOperation(
								"DELETE FROM queue_to WHERE rowid IN({0})".format(','.join(delete)),
							))

							# Add to queue
							for queue in queues:
								self.queuePut(queue)

					def _e(result, self=self):
						err(result)

						# Wait if error
						return self.sleepOneWithFireOnServiceStop(2)

					if (self._process < self.queueWorkers) and (len(self.queue.pending) < self.queueWorkers):
						if self.isStarted:
							yield _c().addErrback(_e)
					else:
						# Wait
						if self.isStarted:
							yield sleep(0.1)
				except:
					err()

			msg(self.name, 'stops queueGenerator', system='-')
		finally:
			self._fetcher -= 1

	def queuePut(self, item):
		self.queue.put(item)

	def queueWorker(self, number):
		self._workers += 1

		# Try
		try:
			msg(self.name, 'start queueWorker #%d' % (
				number), system='-')

			while self.isStarted:
				deferred = self.queue.get()
				deferred.addCallback(self.queueProcess)

				# Wait for next item
				yield deferred.addErrback(err)
				yield sleep(self.senderIntervalNext)

			msg(self.name, 'stops queueWorker #%d' % (
				number), system='-')
		finally:
			self._workers -= 1

	_re_attach_images_html = re.compile(u'<(?:img)[^>]+((src)\s*=\s*([\'\"]?)((?:https?:\/\/)[^>\'\"]+)[\'\"]?)', re.I | re.S).findall

	@inlineCallbacks
	def queueProcess(self, item):
		if (not self.isStarted) or (item is self.queueStop):
			# Stop
			returnValue(None)

		self._process += 1

		# Try
		try:
			charsetMessage = 'UTF-8'
			charsetIn = 'utf8'

			# Debug
			(msg(self.name,
				'queueProcess item', item['id'], system='-'))

			try:
				# Data mail
				id = str(uuid4())
				current = dict()

				# From email
				if 'from' in item['message']['params'] and isinstance(item['message']['params']['from'], DictType):
					current['fEmail'] = item['message']['params']['from']['email']
					current['fNames'] = item['message']['params']['from']['name']

					if not isinstance(current['fNames'], UnicodeType):
						current['fNames'] = current['fNames'].decode(charsetIn, 'replace')
				else:
					# Fail
					raise RuntimeError('Wow! Wrong from {0}'.format(repr(item)))

				# To email
				if item['name']:
					current['tEmail'] = item['email']
					current['tNames'] = item['name']

					if not isinstance(current['tNames'], UnicodeType):
						current['tNames'] = current['tNames'].decode(charsetIn, 'replace')
				else:
					current['tEmail'] = item['email']
					current['tNames'] = None

				# Reply-To email
				if 'reply-to' in item['message']['params'] and item['message']['params']['reply-to']:
					if isinstance(item['message']['params']['reply-to'], DictType):
						current['rEmail'] = item['message']['params']['reply-to']['email']
						current['rNames'] = item['message']['params']['reply-to']['name']

						if not isinstance(current['rNames'], UnicodeType):
							current['rNames'] = current['rNames'].decode(charsetIn, 'replace')
					else:
						current['rEmail'] = item['message']['params']['reply-to']
						current['rNames'] = None
				else:
					current['rEmail'] = None
					current['rNames'] = None

				current['body'] = MIMEMultipart('related')
				# current['body']['X-Sender'] = 'mail-services'

				if item['message']['subject']:
					current['subject'] = item['message']['subject']

					if not isinstance(current['subject'], UnicodeType):
						current['subject'] = text.decode('utf8', 'replace')

					current['body']['Subject'] = Header(current['subject'], charsetMessage)

				current['body']['From'] = ('%s <%s>' % (
					Header(current['fNames'], charsetMessage),
					current['fEmail']
				))

				# To
				if current['tNames']:
					current['body']['To'] = ('%s <%s>' % (
						Header(current['tNames'], charsetMessage),
						current['tEmail']
					))
				else:
					current['body']['To'] = current['tEmail']

				if current['rEmail']:
					if current['rNames']:
						current['body']['Reply-To'] = ('%s <%s>' % (
							Header(current['rNames'], charsetMessage),
							current['rEmail']
						))
					else:
						current['body']['Reply-To'] = current['rEmail']

				current['body'].add_header('Message-ID', '<%s-%s@%s>' % (
					id, item['id'], 'localhost'
				))

				current['parts'] = []

				# Get messages
				current['html'] = item['message'].pop('html')
				current['text'] = item['message'].pop('text')

				if current['text']:
					if not isinstance(current['text'], UnicodeType):
						current['text'] = current['text'].decode(charsetIn, 'replace')

				if current['html']:
					if not isinstance(current['html'], UnicodeType):
						current['html'] = current['html'].decode(charsetIn, 'replace')

				# Attach images
				if config.getboolean('sender', 'attach-images'):
					if current['html']:
						images_url = self._re_attach_images_html(current['html'])
						images_url = map(lambda row: dict(source=row[0], type=row[1], separator=row[2], url=row[3].encode(CHARSET)), images_url)

						if images_url:
							images = defaultdict(list)

							# Group images
							for image_url in images_url:
								images[image_url['url']].append(dict(
									source=image_url['source'],
									type=image_url['type'],
									separator=image_url['separator'],
								))

							# Transfort images to list, with access by index
							images = list(images.items())

							if DEBUG:
								(msg(self.name,
									'queueProcess item', item['id'], 'download images', len(images), 'sources', len(images_url), system='-'))

							# Clean
							del images_url

							semaphoreImages = DeferredSemaphore(5)
							deferredsImages = [semaphoreImages.run(self.downloadToTemporary, url=url) for (url, sources) in images]

							for i, (status, result) in enumerate((yield DeferredList(deferredsImages, fireOnOneErrback=False, consumeErrors=True))):
								if not status:
									# Skip errors
									continue

								# Replace in content
								for source in images[i][1]:
									current['html'] = current['html'].replace(source['source'], '{type}={separator}{info[url]}{separator}'.format(
										type=source['type'], 
										separator=source['separator'],
										**result
									))

							if DEBUG:
								(msg(self.name,
									'queueProcess item', item['id'], 'download images', 'ok', system='-'))

				if current['text'] and current['html']:
					message = MIMEMultipart('alternative')

					message.attach(MIMEText(current['text'], 'plain', _charset=charsetMessage))
					message.attach(MIMEText(current['html'], 'html', _charset=charsetMessage))

				elif current['text'] or current['html']:
					if current['text']:
						message = MIMEText(current['text'], 'plain', _charset=charsetMessage)

					if current['html']:
						message = MIMEText(current['html'], 'html', _charset=charsetMessage)
				else:
					# Fail
					raise RuntimeError('Wow! Wrong message {0}'.format(repr(item)))

				# Add message
				current['parts'].insert(0, message)

				for part in current['parts']:
					current['body'].attach(part)

				deferred = Deferred()
				resolver = Deferred()

				# Try send
				contextFactory = ClientContextFactory()
				contextFactory.method = SSLv3_METHOD

				file = StringIO(str(current['body'].as_string()))
				file.seek(0)

				# if DEBUG:
				# 	(msg(self.name,
				# 		'queueProcess item', item['id'], file.getvalue(), current, system='-'))

				# Clean
				del current
				del id

				if DEBUG_SENDER:
					deferred.callback('OK')
				else:
					factory = (ESMTPSenderFactory(
						username=config.get('smtp', 'username'),
						password=config.get('smtp', 'password'),
						fromEmail=current['fEmail'],
						toEmail=(current['tEmail'], ),
						deferred=deferred,
						file=file,
						contextFactory=False,
						requireTransportSecurity=False,
						retries=3,
						timeout=10,
					))

					if config.getboolean('smtp', 'ssl'):
						(reactor.connectSSL(
							config.get('smtp', 'hostname'),
							config.getint('smtp', 'portname'),
							factory,
							contextFactory,
							timeout=10,
						))
					else:
						(reactor.connectTCP(
							config.get('smtp', 'hostname'),
							config.getint('smtp', 'portname'),
							factory,
							timeout=10,
						))

				result = ((yield deferred))

				if item['group']:
					# Update group counters
					(yield sqlite.runOperation(
						"UPDATE queue_groups SET wait = (wait - 1), sent = (sent + 1) WHERE rowid = ?",
						(item['group'], )
					))

				# Debug
				(msg(self.name,
					'queueProcess item', item['id'], 'sent', repr(result), system='-'))
			except Exception, e:
				err()

				# Debug
				(msg(self.name,
					'queueProcess item', item['id'], 'fallback', system='-'))

				if item['group']:
					# Update group counters
					(yield sqlite.runOperation(
						"UPDATE queue_groups SET wait = (wait - 1), errors = (errors + 1) WHERE rowid = ?",
						(item['group'], )
					))
		finally:
			self._process -= 1

	_current_download = dict()
	_current_download_urls = set()

	@inlineCallbacks
	def downloadToTemporary(self, url):
		if (not self.isStarted):
			# Stop
			returnValue(None)

		hash = hashlib.md5(url).hexdigest()

		file = tmp('f_{0}'.format(hash))
		fileTmp = '{0}.tmp'.format(file)
		fileInf = '{0}.inf'.format(file)

		if hash in self._current_download:
			if self._current_download[hash] is None:
				self._current_download[hash] = Deferred()

			returnValue(self._current_download[hash])
		else:
			results = None
			success = None

			if os.path.exists(file):
				# Read file info
				with open(fileInf, 'rb') as fp:
					info = (dict(
						**loads(fp.read())
					))

				# Success
				success = (dict(
					file=file,
					info=info,
				))
			else:
				if DEBUG:
					(msg(self.name,
						'downloadToTemporary url', repr(url), 'hash', repr(hash), system='-'))

				try:
					self._current_download[hash] = None
					self._current_download_urls.add(url)

					headers = Headers()
					headers.setRawHeaders('X-Sender', ['{0}'.format(hash)])

					# Download file to self tmp
					response = (yield self._agent.request(
						'GET',
						url,
						headers=headers,
						bodyProducer=None
					))

					if response.code not in (OK, PARTIAL_CONTENT):
						transfer = Deferred()
						protocol = BufferProtocol(transfer)

						response.deliverBody(protocol)

						# Fetch file
						((yield transfer))

						# Check response code
						raise HTTPError(code=response.code)

					contentType = response.headers.getRawHeaders('content-type')
					contentType = contentType[0] if contentType else None

					if contentType:
						# Parse
						contentType = contentType.split(';').pop(0)
						contentType = contentType.split('/')
						contentType = map(lambda row: str(row), contentType)

					if not contentType or not len(contentType) == 2:
						# Wrong
						raise RuntimeError('Wrong content type "{0}"'.format(contentType))

					with open(fileTmp, 'wb') as fp:
						transfer = Deferred()
						protocol = FileProtocol(transfer, fp)

						response.deliverBody(protocol)

						# Fetch file
						((yield transfer))

					# Move tmp file to normal
					os.rename(fileTmp, file)

					# File info
					info = (dict(
						url=url,
						type=contentType,
					))

					# Write file info
					with open(fileInf, 'wb') as fp:
						fp.write(dumps(info))

					# Success
					success = (dict(
						file=file,
						info=info,
					))

					if self._current_download[hash]:
						self._current_download[hash].callback(success)
				except Exception, e:
					traceback = sys.exc_info()[2]

					if not traceback:
						traceback = None

					# Remove files
					for i in (file, fileTmp, fileInf):
						if os.path.exists(i):
							try:
								os.remove(i)
							except:
								err()

					if DEBUG:
						(msg(self.name,
							'downloadToTemporary url', repr(url), 'hash', repr(hash), 'error', repr(e), system='-'))

					if self._current_download[hash]:
						self._current_download[hash].errback(e)

					# Fail
					raise e, None, traceback
				finally:
					# Clean
					try:
						self._current_download.pop(hash)
						self._current_download_urls.remove(url)
					except:
						err()

			if success is not None:
				returnValue(success)

