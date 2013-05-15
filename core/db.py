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
from collections import deque, defaultdict
from time import time

from twisted.python import log
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.address import IPv4Address, UNIXAddress
from twisted.internet.task import LoopingCall

from core.constants import DEBUG, QUEUE_PAUSE_PRIORITY, QUEUE_UNPAUSE_PRIORITY, QUEUE_MAX_PRIORITY
from core.constants import GROUP_STATUS_ACTIVE, GROUP_STATUS_PAUSED, GROUP_STATUS_INACTIVE
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

	def getData(self):
		if self.data:
			return self.data

		# Default
		return self.empty()

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

	def delete(self, id, force=True):
		message = self.data.get(id)

		# Delete from seld
		if force:
			try:
				del self.data[id]

				# Changes
				self.changesOne += 1
				self.changesAll += 1
			except KeyError:
				# Skip
				pass
		else:
			del self.data[id]

			# Changes
			self.changesOne += 1
			self.changesAll += 1

		# Clean object
		if message is not None:
			message.delete()

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

		if to.group:
			group = groups.get(to.group)

			if group:
				# Check group status
				assert group.status != GROUP_STATUS_INACTIVE
				
				if group.status == GROUP_STATUS_PAUSED:
					to.priority = QUEUE_PAUSE_PRIORITY + to.priority

			# Clean
			del group

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
		to = None
		ps = None

		if self.data[1]:
			# Check statuses
			if self.data[1][0][0] <= QUEUE_MAX_PRIORITY:
				to = heappop(self.data[1])[1]

		if self.data[0]:
			to = self.data[0].popleft()

		# Found
		if to is not None:
			# Changes
			self.changesOne += 1
			self.changesAll += 1

			# Success
			return to

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

				startup, to1 = data[0]
				if startup <= current:
					# if DEBUG:
					# 	log.msg(self, 'checkAfter', 'startup', to1, 'got', startup, current)

					after, to2 = heappop(data)

					# if DEBUG:
					# 	log.msg(self, 'checkAfter', 'startup', to1, to2, 'rotate')

					if to1 is to2:
						if to2.group:
							group = groups.get(to2.group)

							if group:
								# Check group status
								if group.status == GROUP_STATUS_PAUSED:
									to2.priority = QUEUE_PAUSE_PRIORITY + to2.priority

							# Clean
							del group

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

	def statusForGroup(self, group, status):
		if status == GROUP_STATUS_ACTIVE:
			if DEBUG:
				log.msg(self, 'statusForGroup', group, 'active')

			# A queue
			aq = self.data[1][:]
			aa = None

			# B queue
			bq = self.data[0]
			ba = bq.append

			if aq:
				# Update
				self.data[1][:] = []

				# C queue
				cq = self.data[1]
				ca = cq.append

				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'aq', len(aq))

				for (a, b) in aq:
					if a >= QUEUE_PAUSE_PRIORITY and b.group == group:
						# Paused
						b.priority = b.priority - QUEUE_PAUSE_PRIORITY

						# If priority
						if b.priority > 0:
							# Update
							ca((b.priority, b))
						else:
							ba(b)
					else:
						ca((a, b))

				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'aq', 'ok')

				# Transform list into a heap
				heapify(cq)

			if DEBUG:
				log.msg(self, 'statusForGroup', group, 'active', 'ok')
		elif status == GROUP_STATUS_PAUSED:
			if DEBUG:
				log.msg(self, 'statusForGroup', group, 'paused')

			# A queue
			aq = self.data[1]
			aa = aq.append

			# B queue
			bq = list(self.data[0])
			ba = bq.append

			if aq:
				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'aq', len(aq))

				for i, (a, b) in enumerate(aq):
					if a <= QUEUE_MAX_PRIORITY and b.group == group:
						# Paused
						b.priority = b.priority + QUEUE_PAUSE_PRIORITY

						# Update
						aq[i] = (b.priority, b)

				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'aq', 'ok')

			if bq:
				# Update
				self.data[0].clear()

				# C queue
				cq = self.data[0]
				ca = cq.append

				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'bq', len(bq))

				for (b) in bq:
					if b.group == group:
						# Paused
						b.priority = QUEUE_PAUSE_PRIORITY

						# Update
						aa((b.priority, b))
					else:
						ca(b)

				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'bq', 'ok')

			# Transform list into a heap
			heapify(aq)

			if DEBUG:
				log.msg(self, 'statusForGroup', group, 'paused', 'ok')
		elif status == GROUP_STATUS_INACTIVE:
			if DEBUG:
				log.msg(self, 'statusForGroup', group, 'inactive')

			# A queue
			aq = self.data[1][:]
			aa = None

			# B queue
			bq = list(self.data[0])
			ba = bq.append

			if aq:
				# Update
				self.data[1][:] = []

				# C queue
				cq = self.data[1]
				ca = cq.append

				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'aq', len(aq))

				for (a, b) in aq:
					if b.group == group:
						# Update
						if b.message:
							message = messages.get(b.message)

							if message is not None:
								# Update tos count
								message.tos -= 1
					else:
						ca((a, b))

				# Transform list into a heap
				heapify(cq)

				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'aq', 'ok')

			if bq:
				# Update
				self.data[0].clear()

				# C queue
				cq = self.data[0]
				ca = cq.append

				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'bq', len(bq))

				for (b) in bq:
					if b.group == group:
						# Update
						if b.message:
							message = messages.get(b.message)

							if message is not None:
								# Update tos count
								message.tos -= 1
					else:
						ca(b)

				if DEBUG:
					log.msg(self, 'statusForGroup', group, 'bq', 'ok')

			if DEBUG:
				log.msg(self, 'statusForGroup', group, 'inactive', 'ok')
		else:
			raise RuntimeError('Unknown status {0}'.format(status))
		
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

	def status(self, group, status):
		group = self.data[group]

		if status == GROUP_STATUS_ACTIVE or status == GROUP_STATUS_PAUSED or status == GROUP_STATUS_INACTIVE:
			if group.status != status:
				# Changed
				tos.statusForGroup(group.id, status)

			# Update
			group.status = status
		else:
			raise RuntimeError('Unknown status {0}'.format(status))

		# Success
		return group.status

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
