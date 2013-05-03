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

from itertools import count
from marshal import dump as mdump, load as mload
from cPickle import load, dump
from time import time
from math import ceil

from twisted.python.log import msg, err
from twisted.internet.reactor import callLater
from twisted.internet import reactor

from core.constants import DEBUG
from core.dirs import tmp, dbs


class Base(object):

	@classmethod
	def fromDict(cls, params):
		return cls(**params)

	def toDict(self):
		raise NotImplementedError


class BaseWithStorage(Base):

	prefix = None

	def path(self, name):
		id = ceil(self.id / 1000.)

		path = dbs('{0:.0f}'.format(ceil((id) / 100.)), '{0:.0f}'.format(id))
		file = os.path.join(path, '{0}_{1}_{2}'.format(self.prefix, self.id, name))

		return ((
			path,
			file
		))

	def set(self, name, value):
		if self.id is None:
			raise RuntimeError('Cannot set with ID none')

		if value is not None:
			path, file = self.path(name)

			if not os.path.exists(path):
				os.makedirs(path, 0777)
				os.chmod(path, 0777)

			with open(file, 'wb') as fp:
				mdump(value, fp)

	def get(self, name):
		if self.id is None:
			raise RuntimeError('Cannot get with ID none')

		if self._cachedLife:
			# Try find in cache
			result = self._cachedGet(name)
			if not result is self._cachedNone:
				# Success
				return result

		path, file = self.path(name)

		if os.path.exists(file):
			with open(file, 'rb') as fp:
				result = mload(fp)

				# If cache enabled, set it
				if self._cachedLife:
					self._cachedSet(name, result)

				# Return to user, not from cache
				return result

	_cached = None

	_cachedNone = object()
	_cachedLife = 20
	_cachedTime = None

	def _cachedSet(self, name, value):
		if DEBUG:
			msg(self, '_cachedSet', name)

		if self._cached is None:
			# Init
			self._cached = dict()
			self._cachedTime = dict()

		if (name in self._cachedTime and self._cachedTime[name] is not None):
			if self._cachedTime[name].active():
				self._cachedTime[name].cancel()
			
			# Clean
			self._cachedTime[name] = None

		self._cached[name] = value
		self._cachedTime[name] = callLater(self._cachedLife, self._cachedDelete, name)

	def _cachedGet(self, name):
		if DEBUG:
			msg(self, '_cachedGet', name)

		if self._cached is not None and name in self._cached:
			if (name in self._cachedTime and self._cachedTime[name] is not None):
				if self._cachedTime[name].active():
					if DEBUG:
						msg(self, '_cachedGet', name, 'reset')

					self._cachedTime[name].reset(self._cachedLife)

			return self._cached[name]

		return self._cachedNone

	def _cachedDelete(self, name):
		if DEBUG:
			msg(self, '_cachedDelete', name)

		if self._cached is not None and name in self._cached:
			del self._cached[name]

			# Clean
			if not self._cached:
				self._cached = None

		if (name in self._cachedTime and self._cachedTime[name] is not None):
			if self._cachedTime[name].active():
				self._cachedTime[name].cancel()
			
			# Clean
			self._cachedTime[name] = None
			if not self._cachedTime:
				self._cachedTime = None


class Message(BaseWithStorage):

	prefix = 'm'

	id = None
	time = None
	params = None
	sender = None

	available = ((
		'id',
		'subject',
		'time',
		'text',
		'html',
		'sender',
		'params',
	))

	def __init__(self, **params):
		if len(params):
			if 'id' in params:
				self.id = params.pop('id')

			for key, value in params.iteritems():
				if not key in self.available:
					raise RuntimeError('Unknown key for Message {0}'.format(key))

				setattr(self, key, value)

			# Default
			if self.time is None:
				self.time = int(reactor.seconds())

	def toDict(self):
		return (dict(
			id=self.id,
			time=self.time,
			params=self.params,
			sender=self.sender
		))

	@property
	def subject(self):
		return self.get('subject')

	@subject.setter
	def subject(self, value):
		return self.set('subject', value)

	@property
	def html(self):
		return self.get('html')

	@html.setter
	def html(self, value):
		return self.set('html', value)

	@property
	def text(self):
		return self.get('text')

	@text.setter
	def text(self, value):
		return self.set('text', value)


class To(Base):

	id = None
	message = None
	group = None
	email = None
	name = None
	time = None
	parts = None
	after = None
	priority = 0
	retries = None

	available = ((
		'id',
		'message',
		'group',
		'email',
		'name',
		'time',
		'parts',
		'after',
		'priority',
		'retries',
	))

	def __init__(self, **params):
		if len(params):
			for key, value in params.iteritems():
				if not key in self.available:
					raise RuntimeError('Unknown key for To {0}'.format(key))

				setattr(self, key, value)

			# Default
			if self.time is None:
				self.time = int(reactor.seconds())

			if self.retries is None:
				self.retries = 1

	def toDict(self):
		return (dict(
			id=self.id,
			message=self.message,
			group=self.group,
			email=self.email,
			name=self.name,
			time=self.time,
			parts=self.parts,
			after=self.after,
			priority=self.priority,
			retries=self.retries,
		))


class Group(Base):

	id = None
	all = 0
	wait = 0
	sending = 0
	sent = 0
	errors = 0
	time = None

	available = ((
		'id',
		'all',
		'wait',
		'sending',
		'sent',
		'errors',
		'time',
	))

	def __init__(self, **params):
		if len(params):
			for key, value in params.iteritems():
				if not key in self.available:
					raise RuntimeError('Unknown key for Group {0}'.format(key))

				setattr(self, key, value)

			# Default
			if self.time is None:
				self.time = int(reactor.seconds())

	def toDict(self):
		return (dict(
			id=self.id,
			all=self.all,
			wait=self.wait,
			sending=self.sending,
			sent=self.sent,
			errors=self.errors,
			time=self.time,
		))
