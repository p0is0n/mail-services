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

from core.db import messages, tos, groups
from core.dirs import tmp
from core.constants import DEBUG, DEBUG_DUMPS, CHARSET, QUEUE_MAX_PRIORITY
from core.constants import GROUP_STATUS_ACTIVE, GROUP_STATUS_INACTIVE, GROUP_STATUSES, GROUP_STATUSES_NAMES
from core.mappers import Message, To, Group


class ReceiverError(Exception):
	"""Receiver error"""


class ReceiverProtocol(Int32StringReceiver):

	MAX_LENGTH = 999999
	COMMANDS_MASK = 'commands_{0}'.format

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
		if DEBUG_DUMPS:
			(msg(
				self.factory.service.name,
				'send',
				repr(data)
			))

		self.sendString(dumps(data))

	def stringReceived(self, data):
		if DEBUG_DUMPS:
			(msg(
				self.factory.service.name,
				'received',
				repr(data)
			))

		# Data json
		data = (loads(data))

		# Data must be only is dict type
		assert isinstance(data, DictType)

		self.sequence += 1
		self.process(data)

	def process(self, item):
		self.factory.service._workers += 1

		if DEBUG_DUMPS:
			(msg(
				self.factory.service.name,
				'process',
				item
			))

		# Default
		result = None
		status = 0

		try:
			itemId = str(item.get('id', self.sequence))
			itemCommand = item['command']

			result = (self.commands(
				itemId,
				itemCommand,
				item
			))
		except ReceiverError, e:
			self.send(dict(
				error=e.message,
				id=itemId,
			))
		except:
			err()

			self.send(dict(
				error='Unknown error, please check you params',
				id=itemId,
			))

		if result is not None:
			try:
				if isinstance(result, Deferred):
					result.addCallback(self._cb_process, id=itemId)
					result.addErrback(self._eb_process, id=itemId)
				else:
					raise RuntimeError('Returning a value other than None, return {0}'.format(
						repr(result)
					))
			except Exception, e:
				traceback = sys.exc_info()[2]

				if not traceback:
					traceback = None

				self.factory.service._workers -= 1

				# Throw
				raise e, None, traceback
		else:
			self.factory.service._workers -= 1

	def _cb_process(self, result, id):
		self.factory.service._workers -= 1

	def _eb_process(self, result, id):
		self.factory.service._workers -= 1

		# Process
		if result.check(ReceiverError):
			self.send(dict(
				error=result.value.message,
				id=id,
			))
		else:
			err(result)

			self.send(dict(
				error='Unknown error, please check you params',
				id=id,
			))

	def commands(self, id, command, item):
		return (getattr(self, self.COMMANDS_MASK(command), self.commands_default)(
			id,
			item
		))

	def commands_default(self, id, item):
		return fail(ReceiverError('Unknown command'))

	def commands_ping(self, id, item):
		self.send(dict(
			ping='pong',
			id=id,
		))

	def commands_group(self, id, item):
		itemType = 'stats' if item.has_key('ids') else 'insert'
		itemGroup = item['ids'] if itemType == 'stats' else None

		if itemType == 'insert':
			response = (dict(
				id=id
			))

			if not isinstance(item['group'], DictType):
				raise ReceiverError('Value "group" must be dictonary type')

			if (not item['group'].get('id')):
				raise ReceiverError('Value "group" must be contains "id" field')

			if 'status' in item['group'] and not item['group']['status'] in GROUP_STATUSES_NAMES:
				raise ReceiverError('Value "group" must be contains valid "status" field, values {0}'.format(GROUP_STATUSES.values()))

			# Check group
			group = groups.get(item['group']['id'])

			# Insert 
			if not group:
				group = Group.fromDict(dict(id=item['group'].pop('id'), **item['group']))
			else:
				if 'status' in item['group']:
					# Update status
					groups.status(group.id, item['group']['status'])
				else:
					self.send(dict(
						error='Group already "{0}" exists'.format(group.id),
						id=itemId,
					))

			response['group'] = (dict(
				id=groups.add(group),
				status=group.status,
			))

			self.send(response)
		elif itemType == 'stats':
			itemGroup = (itemGroup,) if not isinstance(itemGroup, (TupleType, ListType)) else itemGroup
			itemGroup = map(int, itemGroup)

			self.send(dict(
				groups=dict(((group.id, group.toDict()) for group in map(groups.get, itemGroup) if group)),
				id=id,
			))
		else:
			self.send(dict(
				error='Unknown type "{0}"'.format(itemType),
				id=itemId,
			))

	def commands_message(self, id, item):
		itemType = 'stats' if item.has_key('ids') else 'insert'
		itemMessage = item['ids'] if itemType == 'stats' else None

		if itemType == 'insert':
			response = (dict(
				id=id
			))

			if not isinstance(item['message'], DictType):
				raise ReceiverError('Value "message" must be dictonary type')

			if (not item['message'].get('subject')):
				raise ReceiverError('Value "message" must be contains "subject" field')

			if (not item['message'].get('html')) and (not item['message'].get('text')):
				raise ReceiverError('Value "message" must be contains "html" and/or "text" fields')

			if (not item['message'].get('sender')):
				raise ReceiverError('Value "message" must be contains "sender" field')

			if not isinstance(item['message']['sender'], DictType):
				raise ReceiverError('Value "message" field "sender" must be dictonary type')

			if item['message'].get('headers') and not isinstance(item['message']['headers'], DictType):
				raise ReceiverError('Value "message" field "headers" must be dictonary type')

			messageParams = dict((

			))

			# Add params
			if item['message'].get('headers'):
				messageParams['headers'] = item['message']['headers']
			else:
				messageParams['headers'] = dict()

			message = Message.fromDict(dict(id=messages.id, **item['message']))
			message.params = messageParams

			response['message'] = (dict(
				id=messages.add(message),
			))

			self.send(response)
		else:
			self.send(dict(
				error='Unknown type "{0}"'.format(itemType),
				id=itemId,
			))

	def commands_mail(self, id, item):
		itemType = item.get('type', 'single')
		itemGroup = int(item.get('group')) if (item.has_key('group') and item['group']) else None

		response = (dict(
			counts = (dict(
				all=0,
				queued=0
			)),
			id=id
		))

		if itemType == 'single' or itemType == 'multiple':
			# Check
			if not 'to' in item:
				raise ReceiverError('Value "to" missing')

			if not isinstance(item['to'], (ListType, TupleType)):
				item['to'] = [item['to']]

			for to in item['to']:
				if not isinstance(to, DictType):
					raise ReceiverError('Value "to" must be dictonary type')

				if (not to['email']) or (not to['name']) or (int(to.get('priority', 0)) < 0):
					raise ReceiverError('Value "to" must have "email" and "name" fields')

				to['priority'] = min(QUEUE_MAX_PRIORITY, int(to.get('priority', 0)))
				to['priority'] = (-(to['priority'] - QUEUE_MAX_PRIORITY) + 1) if to['priority'] > 0 else 0

				# Create parts
				if not 'parts' in to:
					to['parts'] = dict()

				if not isinstance(to['parts'], DictType):
					raise ReceiverError('Value "parts" in "to" must be dictonary type')

				if 'delay' in to:
					to['after'] = int(reactor.seconds() + to['delay'])

					# Clean
					del to['delay']

			if not isinstance(item['message'], DictType):
				raise ReceiverError('Value "message" must be dictonary type')

			if not 'id' in item['message']:
				if (not item['message'].get('subject')):
					raise ReceiverError('Value "message" must be contains "subject" field')

				if (not item['message'].get('html')) and (not item['message'].get('text')):
					raise ReceiverError('Value "message" must be contains "html" and/or "text" fields')

				if (not item['message'].get('sender')):
					raise ReceiverError('Value "message" must be contains "sender" field')

				if not isinstance(item['message']['sender'], DictType):
					raise ReceiverError('Value "message" field "sender" must be dictonary type')

				if item['message'].get('headers') and not isinstance(item['message']['headers'], DictType):
					raise ReceiverError('Value "message" field "headers" must be dictonary type')

			messageId = item['message']['id'] if 'id' in item['message'] else None
			messageId = int(messageId) if messageId is not None else None

			# Create message
			if messageId is None:
				messageParams = dict((

				))

				# Add params
				if item['message'].get('headers'):
					messageParams['headers'] = item['message']['headers']
				else:
					messageParams['headers'] = dict()

				message = Message.fromDict(dict(id=messages.id, **item['message']))
				message.params = messageParams

				# Add to database
				messageId = messages.add(message)
			else:
				message = messages.get(messageId)
				if message is None:
					raise ReceiverError('Value "message" {0} not found'.format(messageId))

			if itemGroup:
				# Check group
				group = groups.get(itemGroup)
				if group is None or group.status == GROUP_STATUS_INACTIVE:
					raise ReceiverError('Value "group" {0} not found or inactive'.format(itemGroup))
			else:
				group = None

			for to in item['to']:
				to = To.fromDict(to)
				to.message = messageId

				if group is not None:
					to.group = group.id

				# Add to database
				toId = tos.add(to)

				# Counts
				response['counts']['all'] += 1
				response['counts']['queued'] += 1

			if response['counts']['queued'] > 0:
				# Update group
				if group is not None:
					group.all += response['counts']['queued']
					group.wait += response['counts']['queued']

				# Update message
				message.tos += response['counts']['queued']

			self.send(response)
		else:
			self.send(dict(
				error='Unknown type "{0}"'.format(itemType),
				id=itemId,
			))


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

