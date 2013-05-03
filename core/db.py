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

import sys
import os

from marshal import dump as mdump, load as mload
from itertools import count
from heapq import heappush, heappop, heapify
from collections import deque
from time import time

from twisted.python import log
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.address import IPv4Address, UNIXAddress
from twisted.internet.task import LoopingCall

from core.constants import DEBUG
from core.configs import config
from core.dirs import dbs, tmp
from core.mappers import Message, To, Group


class Base:

	def __init__(self, name):
		self.name = name
		
		self.next = self.counter(1)
		self.data = self.empty()
		self.file = dbs('{0}.db'.format(self.name))

		self.changesOne = 0
		self.changesAll = 0

		self.lastSyncTime = time()

	@property
	def id(self):
		return self.next.next()

	def load(self):
		raise NotImplementedError

	def save(self):
		raise NotImplementedError

	def empty(self):
		raise NotImplementedError

	def counter(self, start):
		return count(start, step=1)

	def startSync(self):
		self.loopingSync = LoopingCall(self.sync)
		self.loopingSync.start(60, True)

	def stopSync(self):
		self.loopingSync.stop()
		self.loopingSync = None

	def sync(self):
		mark = reactor.seconds()
		last = self.lastSyncTime

		try:
			for seconds, changes in config.dbSync():
				if self.changesOne >= changes:
					if last <= (mark - seconds):
						log.msg(self, 'sync', seconds, changes)

						# @TODO: This code block all main-loop, fix-it
						self.save()

						log.msg(self, 'sync', 'ok')

						# Update
						self.changesOne = 0
						self.lastSyncTime = mark

						# Exit
						break
		except:
			log.err()

	def __str__(self):
		return 'DB-{0}'.format(self.name)


class Messages(Base):

	def __init__(self):
		Base.__init__(self, 'messages')

	def add(self, message):
		assert isinstance(message, Message)

		# Create id if needed
		if message.id is None:
			id = self.id
		else:
			id = message.id
			
			# Update id
			if id > self.id:
				self.next = self.counter(id)

		# Insert
		self.data[id] = message

		# Update
		message.id = id

		# Changes
		self.changesOne += 1
		self.changesAll += 1

		# Success
		return id

	def get(self, id):
		return self.data.get(id)

	def empty(self):
		return dict()

	def load(self):
		log.msg(self, 'load')

		if os.path.exists(self.file):
			# Load
			with open(self.file, 'rb') as fp:
				next, data = mload(fp)

				log.msg(self, 'load', len(data))

				self.next = self.counter(next)
				self.data = dict(((message['id'], Message.fromDict(message)) for message in data))

		log.msg(self, 'load ok')

	def save(self):
		log.msg(self, 'save')
		log.msg(self, 'save', len(self.data))

		# Save
		with open(self.file, 'wb') as fp:
			mdump((self.next.next(), tuple(message.toDict() for id, message in self.data.iteritems())), fp)

		log.msg(self, 'save ok')


class Tos(Base):

	def __init__(self):
		Base.__init__(self, 'tos')

	def add(self, to):
		assert isinstance(to, To)

		# Create id if needed
		if to.id is None:
			id = self.id
		else:
			id = to.id

			# Update id
			if id > self.id:
				self.next = self.counter(id)

		# Insert
		if to.after is not None:
			(heappush(
				self.data[2], 
				(to.after, to)
			))
		elif to.priority > 0:
			(heappush(
				self.data[1], 
				(to.priority, to)
			))
		else:
			self.data[0].append(to)

		# Update
		to.id = id

		# Changes
		self.changesOne += 1
		self.changesAll += 1

		# Success
		return id

	def pop(self):
		if self.data[1]:
			# Changes
			self.changesOne += 1
			self.changesAll += 1

			return heappop(self.data[1])[1]

		if self.data[0]:
			# Changes
			self.changesOne += 1
			self.changesAll += 1

			return self.data[0].pop()

	def checkAfter(self):
		if self.data[2]:
			current = reactor.seconds()
			rotated= 0

			data = self.data[2]

			if DEBUG:
				log.msg(self, 'checkAfter', len(data), 'wait', current)

			while data and rotated < 10000:
				# if DEBUG:
				# 	log.msg(self, 'checkAfter', len(data), 'wait', current)

				startup, to1 =data[0]
				if startup <= current:
					# if DEBUG:
					# 	log.msg(self, 'checkAfter', 'startup', to1, 'got', startup, current)

					after, to2 = heappop(data)

					# if DEBUG:
					# 	log.msg(self, 'checkAfter', 'startup', to1, to2, 'rotate')

					if to1 is to2:
						# Ok, rotate
						if to2.priority > 0:
							(heappush(
								self.data[1], 
								(to2.priority, to2)
							))
						else:
							self.data[0].append(to2)

						# Changes
						self.changesOne += 1
						self.changesAll += 1
					else:
						(heappush(
							data, 
							(after, to2)
						))

					rotated += 1
				else:
					# Ok, wait time
					break

			if DEBUG:
				log.msg(self, 'checkAfter', len(data), 'wait', current, 'rotated', rotated)

	def empty(self):
		return (deque(), [], [])

	def load(self):
		log.msg(self, 'load')

		if os.path.exists(self.file):
			# Load
			with open(self.file, 'rb') as fp:
				next, data = mload(fp)

				log.msg(self, 'load', len(data[0]), len(data[1]), len(data[2]))

				self.next = self.counter(next)
				self.data = ((
					deque(tuple(To.fromDict(to) for to in data[0])), 
					[(priority, To.fromDict(to)) for priority, to in data[1]],
					[(after, To.fromDict(to)) for after, to in data[2]],
				))

				# Transform list into a heap
				heapify(self.data[1])
				heapify(self.data[2])

		log.msg(self, 'load ok')

	def save(self):
		log.msg(self, 'save')
		log.msg(self, 'save', len(self.data[0]), len(self.data[1]), len(self.data[2]))

		# Save
		with open(self.file, 'wb') as fp:
			mdump((self.next.next(), ( 
				tuple(to.toDict() for to in self.data[0]), 
				tuple((priority, to.toDict()) for priority, to in self.data[1]),
				tuple((after, to.toDict()) for after, to in self.data[2]),
			)), fp)

		log.msg(self, 'save ok')


class Groups(Base):

	def __init__(self):
		Base.__init__(self, 'groups')

	def add(self, group):
		assert isinstance(group, Group)

		# Create id if needed
		if group.id is None:
			id = self.id
		else:
			id = group.id

			# Update id
			if id > self.id:
				self.next = self.counter(id)

		# Insert
		self.data[id] = group

		# Update
		group.id = id

		# Changes
		self.changesOne += 1
		self.changesAll += 1

		# Success
		return id

	def get(self, id):
		return self.data.get(id)

	def empty(self):
		return dict()

	def load(self):
		log.msg(self, 'load')

		if os.path.exists(self.file):
			# Load
			with open(self.file, 'rb') as fp:
				next, data = mload(fp)

				log.msg(self, 'load', len(data))

				self.next = self.counter(next)
				self.data = dict(((group['id'], Group.fromDict(group)) for group in data))

		log.msg(self, 'load ok')

	def save(self):
		log.msg(self, 'save')
		log.msg(self, 'save', len(self.data))

		# Save
		with open(self.file, 'wb') as fp:
			mdump((self.next.next(), tuple(group.toDict() for id, group in self.data.iteritems())), fp)

		log.msg(self, 'save ok')


messages = Messages()
tos = Tos()
groups = Groups()

# Handlers
reactor.addSystemEventTrigger('before', 'startup', messages.load)
reactor.addSystemEventTrigger('before', 'startup', tos.load)
reactor.addSystemEventTrigger('before', 'startup', groups.load)

reactor.addSystemEventTrigger('after', 'shutdown', messages.save)
reactor.addSystemEventTrigger('after', 'shutdown', tos.save)
reactor.addSystemEventTrigger('after', 'shutdown', groups.save)

reactor.addSystemEventTrigger('after', 'startup', messages.startSync)
reactor.addSystemEventTrigger('after', 'startup', tos.startSync)
reactor.addSystemEventTrigger('after', 'startup', groups.startSync)

reactor.addSystemEventTrigger('before', 'shutdown', messages.stopSync)
reactor.addSystemEventTrigger('before', 'shutdown', tos.stopSync)
reactor.addSystemEventTrigger('before', 'shutdown', groups.stopSync)
