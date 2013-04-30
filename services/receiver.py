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

from cStringIO import StringIO
from pprint import pprint
from json import loads, dumps
from types import DictType, ListType, TupleType
from time import time
from itertools import chain, islice

from twisted.python.log import msg, err
from twisted.internet.defer import Deferred, DeferredLock, DeferredList, DeferredSemaphore
from twisted.internet.defer import DeferredQueue, inlineCallbacks, returnValue
from twisted.internet.defer import fail, succeed
from twisted.internet.task import cooperate
from twisted.internet.reactor import callLater
from twisted.internet.protocol import ServerFactory
from twisted.internet import reactor
from twisted.internet.endpoints import serverFromString
from twisted.application.internet import StreamServerEndpointService as Service
from twisted.protocols.basic import Int32StringReceiver

from core.db import sqlite
from core.dirs import tmp
from core.constants import DEBUG, DEBUG_DUMPS, CHARSET


class ReceiverError(Exception):
	"""Receiver error"""


class ReceiverProtocol(Int32StringReceiver):

	MAX_LENGTH = 999999

	def connectionMade(self):
		if DEBUG:
			(msg(
				self.factory.service.name,
				'connection made',
			))

		# Create sequence
		self.sequence = 0

	def connectionLost(self, reason):
		if DEBUG:
			(msg(
				self.factory.service.name,
				'connection lost',
			))

	def send(self, data):
		self.sendString(dumps(data))

	def sendError(self, error, id=None):
		self.send(dict(error=error, id=id))

	def stringReceived(self, data):
		if DEBUG_DUMPS:
			(msg(
				self.factory.service.name,
				'received',
				repr(data)
			))

		self.sequence += 1
		self.process(dict( **loads(data) ))

	@inlineCallbacks
	def process(self, item):
		self.factory.service._workers += 1

		# Try
		try:
			if DEBUG_DUMPS:
				(msg(
					self.factory.service.name,
					'process',
					item
				))

			try:
				itemId = str(item.get('id', self.sequence))
				itemCommand = item['command']
				itemTest = bool(item.get('test'))

				if itemCommand == 'mail':
					itemType = item.get('type', 'single')
					itemGroup = int(item.get('group')) if item.has_key('group') else None

					counts = (dict(
						all=0,
						queued=0
					))

					if itemType == 'single' or itemType == 'multiple':
						# Check
						if not isinstance(item['to'], (ListType, TupleType)):
							item['to'] = [item['to']]

						for to in item['to']:
							if not isinstance(to, DictType):
								raise ReceiverError('Value "to" must be dictonary type')

							if (not to['email']) or (not to['name']) or (int(to.get('priority', 0)) < 0):
								raise ReceiverError('Value "to" must have "email" and "name" fields')

							# Create parts
							if not 'parts' in to:
								to['parts'] = dict()

							if not isinstance(to['parts'], DictType):
								raise ReceiverError('Value "parts" in "to" must be dictonary type')

						if not isinstance(item['message'], DictType):
							raise ReceiverError('Value "message" must be dictonary type')

						if (not item['message'].get('subject')):
							raise ReceiverError('Value "message" must be contains "subject" field')

						if (not item['message'].get('html')) and (not item['message'].get('text')):
							raise ReceiverError('Value "message" must be contains "html" and/or "text" fields')

						if (not item['message'].get('from')):
							raise ReceiverError('Value "message" must be contains "from" field')

						if not isinstance(item['message']['from'], DictType):
							raise ReceiverError('Value "message" field "from" must be dictonary type')

						if item.get('headers') and not isinstance(item['headers'], DictType):
							raise ReceiverError('Value "headers" must be dictonary type')

						def _(transaction, item):
							messageId = None
							messageParams = dict((
								('from', dict(
									name=item['message']['from']['name'], 
									email=item['message']['from']['email']
								)),
							))

							if item.get('headers'):
								messageParams['headers'] = item['headers']
							else:
								messageParams['headers'] = dict()

							if itemGroup:
								# Check group
								result = (transaction.execute(
									"SELECT id FROM queue_groups WHERE id = ?",
									((
										itemGroup,
									))
								))

								result = transaction.fetchall()
								result = result[0] if result else None

								if result is None:
									# Insert group
									(transaction.execute(
										"INSERT INTO queue_groups (id) VALUES (?)",
										((
											itemGroup,
										))
									))

							(transaction.execute(
								"INSERT INTO queue_messages (subject, text, html, create_time, params) VALUES (?, ?, ?, ?, ?)",
								((
									item['message']['subject'],
									item['message'].get('text'),
									item['message'].get('html'),
									int(time()),
									dumps(messageParams)
								))
							))

							# Insert multi
							b = 100
							l = item['to']
							c = len(l)
							i = transaction.lastrowid

							for j in xrange(0, c, b):
								k = islice(l, j, j + b)
								h = min(b, (j + b - (b - (c - j))) - j)

								(transaction.execute(
									("INSERT INTO queue_to {0}".format(
										' UNION '.join( (("SELECT ? AS message_id, ? AS group_id, ? AS email, ? AS name, strftime('%s', 'now') AS create_time, ? AS parts, ? AS priority", ) * h) )
									)),
									tuple(chain(
										*((i, itemGroup, to['email'], to['name'], dumps(to['parts']), to.get('priority', 0)) for to in k)
									))
								))

								counts['all'] += h
								counts['queued'] += h

							if counts['queued'] > 0:
								# Update group
								(transaction.execute(
									"UPDATE queue_groups SET wait = (wait + ?) WHERE id = ?",
									((
										counts['queued'],
										itemGroup,
									))
								))

						if not itemTest:
							(yield sqlite.runInteraction(_, item=item))
						else:
							# Run as test, with out updates
							(yield None)

						self.send(dict(
							counts=counts,
							id=itemId,
						))
					else:
						# Error, unknown type
						self.sendError(
							error='Unknown type "{0}"'.format(itemType),
							id=itemId,
						)

				elif itemCommand == 'group':
					itemGroup = item['group'] if item.has_key('group') else item['groups']
					itemGroup = (itemGroup,) if not isinstance(itemGroup, (TupleType, ListType)) else itemGroup
					itemGroup = map(str, map(int, itemGroup))
					
					result = dict()
					groups = (yield sqlite.runQuery(
						"SELECT id, wait, sent, errors FROM queue_groups WHERE id IN({0})".format(','.join(itemGroup)),
					))

					for group in groups:
						result[group[0]] = (dict(
							wait=group[1],
							sent=group[2],
							errors=group[3],
						))

					self.send(dict(
						groups=result,
						id=itemId,
					))

			except ReceiverError, e:
				# Send error to client
				self.sendError(
					error=e.message,
					id=itemId,
				)
			except Exception, e:
				err()

				# Send error to client
				self.sendError(
					error='Unknown error, please check you params',
					id=itemId,
				)
		finally:
			self.factory.service._workers -= 1


class ReceiverFactory(ServerFactory):

	protocol = ReceiverProtocol
	noisy = False

	def __init__(self, service):
		self.service = service


class ReceiverService(Service):

	# DO NOT EDIT!
	semaphore = DeferredLock()

	def __init__(self, listen):
		self.name = 'Receiver'
		self.loop = -1

		# Inside
		self._stopCall = None
		self._stopDeferred = None
		self._workers = 0

		(Service.__init__(
			self,
			endpoint=serverFromString(reactor, listen),
			factory=ReceiverFactory(service=self),
		))

	def startService(self):
		if self.running or self.loop != -1:
			# Already started
			return

		msg(self.name, 'start')

		# Cancel stop
		if self._stopCall:
			self._stopCall.cancel()
			self._stopCall = None

			# Call stop
			self._stopDeferred.callback(0)
			self._stopDeferred = None

		self.loop = 0

		Service.startService(self)

	def stopService(self):
		if (not self.running) or self.loop == -1:
			# Already stopped
			return

		msg(self.name, 'stops')

		# Success deferred
		deferred = Deferred()
		callback = Deferred()

		def s1(result, self=self):
			if self._stopDeferred is None:
				# Fail
				return

			msg(self.name, 'alive workers', self._workers)

			if self._workers:
				# Fail, wait...
				self._stopCall = callLater(5, s1, 0)
			else:
				self._stopCall = callLater(1, deferred.callback, 0)

		def s2(result, self=self):
			if self._stopDeferred is None:
				# Fail
				return

			self._stopCall = None
			self._stopDeferred = None

			# Inside
			Service.stopService(self)

		self.loop = -1

		self._stopCall = callLater(0, s1, 0)
		self._stopDeferred = deferred

		return deferred.addCallback(s2)

