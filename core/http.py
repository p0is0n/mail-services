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
import hashlib

from pprint import pprint
from time import time
from types import StringTypes, UnicodeType
from urllib import urlencode, quote
from cStringIO import StringIO
from UserDict import UserDict
from random import choice
from itertools import cycle

from zope.interface import implements

from twisted.python.log import msg, err
from twisted.python.failure import Failure
from twisted.internet.defer import Deferred, DeferredQueue, fail, TimeoutError
from twisted.internet.task import cooperate
from twisted.internet.protocol import Protocol
from twisted.internet import reactor
from twisted.protocols.policies import TimeoutMixin
from twisted.web.client import getPage, HTTPConnectionPool, Agent, ContentDecoderAgent
from twisted.web.http_headers import Headers
from twisted.web.error import Error
from twisted.web.client import GzipDecoder, CookieAgent, FileBodyProducer, _HTTP11ClientFactory
from twisted.web._newclient import HTTP11ClientProtocol, ResponseDone
from twisted.web.iweb import IBodyProducer

from core.utils import sleep
from core.constants import USERAGENT, DEBUG, CHARSET, DEFAULT_HTTP_TIMEOUT


class HTTPError(Error):
	"""Base HTTP Error"""


class HttpClientProtocol(TimeoutMixin, HTTP11ClientProtocol):

	timeoutCall = None
	timeout = DEFAULT_HTTP_TIMEOUT

	def request(self, request):
		if self.timeout:
			self.setTimeout(self.timeout)

		def _cb(result, self=self):
			# Reset timeouts
			self.setTimeout(None)

			return result

		def _eb(result, self=self):
			# Reset timeouts
			self.setTimeout(None)

			return result

		deferred = HTTP11ClientProtocol.request(self, request)
		deferred.addCallbacks(_cb, _eb)

		# Success
		return deferred

	def connectionMade(self):
		"""
		Called when the connection made.
		"""
		if DEBUG:
			pprint((self, 'connectionMade'))

		if not self.transport.getTcpKeepAlive():
			self.transport.setTcpKeepAlive(1)

	def timeoutConnection(self):
		"""
		Called when the connection times out.
		"""
		if DEBUG:
			pprint((self, 'timeoutConnection'))

		self.setTimeout(None)
		self.transport.loseConnection()


class HttpClientFactory(_HTTP11ClientFactory):

	noisy = DEBUG

	def buildProtocol(self, addr):
		return HttpClientProtocol(self._quiescentCallback)


class HttpConnectionPool(HTTPConnectionPool):

	_factory = HttpClientFactory


class HttpAgent(Agent):

	_protocol = HttpClientProtocol

	def __init__(self, reactor, connectTimeout=None, bindAddress=None):
		pool = HttpConnectionPool(reactor, True)
		pool.maxPersistentPerHost = 10
		pool.cachedConnectionTimeout = 3600
		pool.retryAutomatically = True

		if connectTimeout is None:
			connectTimeout = DEFAULT_HTTP_TIMEOUT

		(super(HttpAgent, self).__init__(
			reactor=reactor,
			connectTimeout=connectTimeout,
			bindAddress=bindAddress,
			pool=pool
		))

	def closeCachedConnections(self):
		return self._pool.closeCachedConnections()


class BufferProtocol(TimeoutMixin, Protocol):

	timeoutCall = None
	timeout = DEFAULT_HTTP_TIMEOUT

	def __init__(self, deferred):
		self._deferred = deferred
		self._buffer = []

	def __del__(self):
		self._deferred = None
		self._buffer = None

	def dataReceived(self, data):
		# if DEBUG:
		# 	pprint((self, 'dataReceived', len(data)))

		# Reset timeouts
		if self.timeout:
			self.setTimeout(self.timeout)

		self._buffer.append(data)

	def connectionMade(self):
		"""
		Called when the connection made.
		"""
		if DEBUG:
			pprint((self, 'connectionMade'))

		if self.timeout:
			self.setTimeout(self.timeout)

	def connectionLost(self, reason):
		"""
		Called when the connection lost.
		"""
		if DEBUG:
			pprint((self, 'connectionLost'))

		# Reset timeouts
		self.setTimeout(None)

		if reason.check(ResponseDone):
			# Ok, request success
			self._deferred.callback(''.join(self._buffer))
		else:
			self._deferred.errback(reason)

	def timeoutConnection(self):
		"""
		Called when the connection times out.
		"""
		if DEBUG:
			pprint((self, 'timeoutConnection'))

		self.setTimeout(None)
		self.transport.stopProducing()

	def __repr__(self):
		return '<BufferProtocol 0x{0:X}>'.format(id(self))


class FileProtocol(TimeoutMixin, Protocol):

	timeoutCall = None
	timeout = DEFAULT_HTTP_TIMEOUT

	def __init__(self, deferred, file):
		self._deferred = deferred
		self._file = file

	def __del__(self):
		self._deferred = None
		self._file = None

	def dataReceived(self, data):
		# if DEBUG:
		# 	pprint((self, 'dataReceived', len(data)))

		# Reset timeouts
		if self.timeout:
			self.setTimeout(self.timeout)

		self._file.write(data)

	def connectionMade(self):
		"""
		Called when the connection made.
		"""
		if DEBUG:
			pprint((self, 'connectionMade'))

		if self.timeout:
			self.setTimeout(self.timeout)

	def connectionLost(self, reason):
		"""
		Called when the connection lost.
		"""
		if DEBUG:
			pprint((self, 'connectionLost'))

		# Reset timeouts
		self.setTimeout(None)

		if reason.check(ResponseDone):
			# Ok, request success
			self._deferred.callback(True)
		else:
			self._deferred.errback(reason)

	def timeoutConnection(self):
		"""
		Called when the connection times out.
		"""
		if DEBUG:
			pprint((self, 'timeoutConnection'))

		self.setTimeout(None)
		self.transport.stopProducing()

	def __repr__(self):
		return '<FileProtocol 0x{0:X}>'.format(id(self))


class StringBodyProducer(FileBodyProducer):

	def __init__(self, string):
		if isinstance(string, UnicodeType):
			string = string.encode(CHARSET)

		FileBodyProducer.__init__(self, StringIO(string))


class ParamsBodyProducer(StringBodyProducer):

	def __init__(self, params):
		if not isinstance(params, StringTypes):
			params = urlencode(dict( (key, value.encode(CHARSET)) for key, value in params.iteritems() ))

		StringBodyProducer.__init__(self, params)

