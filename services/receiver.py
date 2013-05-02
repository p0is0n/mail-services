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
from core.constants import DEBUG, DEBUG_DUMPS, CHARSET
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

				(yield self.commands(itemId, itemCommand, item))
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
		finally:
			self.factory.service._workers -= 1

	def commands(self, id, command, item):
		return (getattr(self, self.COMMANDS_MASK(command), self.commands_default)(
			id,
			item
		))

	def commands_default(self, id, item):
		return fail(ReceiverError('Unknown command'))

	def commands_group(self, id, item):
		itemGroup = item['group'] if item.has_key('group') else item['groups']
		itemGroup = (itemGroup,) if not isinstance(itemGroup, (TupleType, ListType)) else itemGroup
		itemGroup = map(int, itemGroup)
					
		self.send(dict(
			groups=dict(((group.id, group.toDict()) for group in map(groups.get, itemGroup) if group)),
			id=id,
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

			message = Message.fromDict(dict(id=messages.id, **item['message']))
			message.params = dict((

			))

			if item['message'].get('headers'):
				message.params['headers'] = item['message']['headers']
			else:
				message.params['headers'] = dict()

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

				# Create parts
				if not 'parts' in to:
					to['parts'] = dict()

				if not isinstance(to['parts'], DictType):
					raise ReceiverError('Value "parts" in "to" must be dictonary type')

			if not isinstance(item['message'], DictType):
				raise ReceiverError('Value "message" must be dictonary type')

			if not 'id' in item['message']:
				if (not item['message'].get('subject')):
					raise ReceiverError('Value "message" must be contains "subject" field')

				if (not item['message'].get('html')) and (not item['message'].get('text')):
					raise ReceiverError('Value "message" must be contains "html" and/or "text" fields')

				if (not item['message'].get('from')):
					raise ReceiverError('Value "message" must be contains "from" field')

				if not isinstance(item['message']['from'], DictType):
					raise ReceiverError('Value "message" field "from" must be dictonary type')

				if item['message'].get('headers') and not isinstance(item['message']['headers'], DictType):
					raise ReceiverError('Value "message" field "headers" must be dictonary type')

			messageId = item['message']['id'] if 'id' in item['message'] else None
			messageId = int(messageId) if messageId is not None else 0

			# Create message
			if messageId is None:
				message = Message.fromDict(item['message'])
				message.params = dict((

				))

				if item['message'].get('headers'):
					message.params['headers'] = item['message']['headers']
				else:
					message.params['headers'] = dict()

				# Add to database
				messageId = messages.add(message)

			if itemGroup:
				# Check group
				group = groups.get(itemGroup)
				if not group:
					group = Group.fromDict(dict(
						id=itemGroup,
					))

					# Add new
					groups.add(group)
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
					group.wait += response['counts']['queued']			

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
