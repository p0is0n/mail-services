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
from cPickle import load, dump
from time import time
from math import ceil

from core.constants import DEBUG
from core.dirs import tmp, dbs


class Message(object):

	id = None
	subject = None
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
				self.time = int(time())

	def path(self, name):
		id = ceil(self.id / 1000.)

		path = dbs('{0:.0f}'.format(ceil((id) / 100.)), '{0:.0f}'.format(id))
		file = os.path.join(path, 'm_{0}_{1}'.format(self.id, name))

		return ((
			path,
			file
		))

	def set(self, name, value):
		if self.id is None:
			raise RuntimeError('Cannot set with ID none')

		path, file = self.path(name)

		if not os.path.exists(path):
			os.makedirs(path, 0777)
			os.chmod(path, 0777)

		with open(file, 'wb') as fp:
			dump(value, fp)

	def get(self, name):
		if self.id is None:
			raise RuntimeError('Cannot get with ID none')

		path, file = self.path(name)

		if os.path.exists(file):
			with open(file, 'rb') as fp:
				return load(fp)

	@classmethod
	def fromDict(cls, params):
		return cls(**params)

	def toDict(self):
		return (dict(
			id=self.id,
			subject=self.subject,
			time=self.time,
			params=self.params,
			sender=self.sender
		))

	@property
	def html(self):
		return self.get('h')

	@html.setter
	def html(self, value):
		return self.set('h', value)

	@property
	def text(self):
		return self.get('t')

	@text.setter
	def text(self, value):
		return self.set('t', value)


class To(object):

	id = None
	message = None
	group = None
	email = None
	name = None
	time = None
	parts = None
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
				self.time = int(time())

			if self.retries is None:
				self.retries = 1

	@classmethod
	def fromDict(cls, params):
		return cls(**params)

	def toDict(self):
		return (dict(
			id=self.id,
			message=self.message,
			group=self.group,
			email=self.email,
			name=self.name,
			time=self.time,
			parts=self.parts,
			priority=self.priority,
			retries=self.retries,
		))


class Group(object):

	id = None
	wait = 0
	sent = 0
	errors = 0
	time = None

	available = ((
		'id',
		'wait',
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
				self.time = int(time())

	@classmethod
	def fromDict(cls, params):
		return cls(**params)

	def toDict(self):
		return (dict(
			id=self.id,
			wait=self.wait,
			sent=self.sent,
			errors=self.errors,
			time=self.time,
		))
