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
from types import ListType, TupleType, UnicodeType, DictType, StringTypes
from json import loads, dumps
from collections import defaultdict

from twisted.python.log import msg, err
from twisted.internet.defer import Deferred, DeferredList, DeferredLock, DeferredSemaphore
from twisted.internet.defer import DeferredQueue, inlineCallbacks, returnValue
from twisted.internet.defer import fail, succeed
from twisted.internet.task import cooperate, LoopingCall
from twisted.internet.reactor import callLater
from twisted.internet import reactor
from twisted.application.service import Service

from core.db import messages, tos, groups
from core.dirs import tmp
from core.constants import DEBUG, DEBUG_SENDER, CHARSET
from core.utils import sleep
from core.configs import config


class GarbageService(Service):

	# DO NOT EDIT!
	semaphore = DeferredLock()

	def __init__(self):
		self.name = 'Garbage'
		self.loop = -1

		# Inside
		self._stopCall = None
		self._stopDeferred = None

		self._workers = 0
		self._process = 0

		self._state = 'stopped'

		self._runs = dict((
			('messages', (LoopingCall(self.process_messages), config.getint('garbage', 'messages-interval'))),
		))

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

		Service.startService(self)

		self.loop = 0
		self._state = 'started'

		# Start
		for name, (run, interval) in self._runs.iteritems():
			run.start(interval, now=False)

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

		# Stops
		for name, (run, interval) in self._runs.iteritems():
			if run.running:
				run.stop()

		def s1(code, self=self):
			(msg(self.name,
				'alive workers', self._workers,
				'alive process', self._process,
			))

			if (self._process + self._workers) > 0:
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

				self._state = 'stopped'

				# Show info
				msg(self.name, 'stopped', system='-')
			except:
				err()

		return deferred.addCallback(s2)

	@inlineCallbacks
	def process_messages(self):
		if not self.isStarted:
			# Fail
			returnValue(None)

		self._process += 1

		# Try
		try:
			# Debug
			msg(self.name, 'process messages', system='-')

			success = None
			current = reactor.seconds()
			removed = []

			# Short
			delete = messages.delete
			append = removed.append

			# Confog
			oldLast = config.getint('garbage', 'old-last-messages')
			oldTime = config.getint('garbage', 'old-time-messages')

			# Clean messages
			for id, message in messages.getData().iteritems():
				if (message.last and (current - message.last) >= oldLast):
					# Remove message
					if DEBUG:
						msg(self.name, 'process messages', 'remove, old last', id, current - message.last, system='-')

					append(id)
				elif (message.last is None and (current - message.time) >= oldTime):
					# Remove message
					if DEBUG:
						msg(self.name, 'process messages', 'remove, old time', id, current - message.time, system='-')

					append(id)

			# Sleep
			(yield sleep(1))

			for id in removed:
				# Remove message
				delete(id)

			# Debug
			msg(self.name, 'process messages stops', system='-')
		finally:
			self._process -= 1
