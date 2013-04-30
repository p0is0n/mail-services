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
import time
import sqlite3

from twisted.python import log
from twisted.enterprise.adbapi import ConnectionPool as AConnectionPool
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.address import IPv4Address, UNIXAddress

from core.constants import DEBUG
from core.configs import config


class SqliteConnectionPool(AConnectionPool):

	noisy = DEBUG
	reconnect = True

	min = 1 # minimum number of connections in pool
	max = 1 # maximum number of connections in pool

	timeoutConnections = None

	def __init__(self, *a, **k):
		AConnectionPool.__init__(self, 'sqlite3', *a, **k)

	def connect(self):
		"""Return a database connection when one becomes available.

		This method blocks and should be run in a thread from the internal
		threadpool. Don't call this method directly from non-threaded code.
		Using this method outside the external threadpool may exceed the
		maximum number of connections in the pool.

		@return: a database connection from the pool.
		"""

		if self.timeoutConnections is None:
			self.timeoutConnections = dict()

		tid = self.threadID()

		# Try get connection
		connection = self.connections.get(tid)
		if connection is None:
				if self.noisy:
					log.msg('adbapi connecting: %s %s%s' % (self.dbapiName,
							self.connargs or '',
							self.connkw or ''))

				connection = self.dbapi.connect(*self.connargs, **self.connkw)
				if self.openfun != None:
						self.openfun(connection)

				self.connections[tid] = connection

				if self.noisy:
					log.msg('adbapi connected: %s' % (self.dbapiName))

		try:
			self.timeoutConnections[tid].cancel()

			if self.noisy:
				log.msg('adbapi timeout: reset for tid', tid)
		except KeyError:
			# Continue error
			pass

		inactive = config.getfloat('db', 'inactive')
		if inactive:
			self.timeoutConnections[tid] = reactor.callLater(inactive,
				self.timeoutConnection, tid)

		# Success
		return connection

	def timeoutConnection(self, tid):
		del self.timeoutConnections[tid]

		# Try get connection
		connection = self.connections.get(tid)
		if connection is None:
			log.msg('adbapi timeout: connection for tid', tid, 'not found')

			# Fail
			return

		self._close(connection)

		try:
			del self.connections[tid]
		except KeyError:
			# Continue error
			pass

		log.msg('adbapi timeout: connection for tid', tid)

	def openfun(self, connection):
		cursor = connection.cursor()

		try:
			cursor.execute("PRAGMA journal_mode=MEMORY")
			cursor.execute("PRAGMA temp_store=MEMORY")
			cursor.execute("PRAGMA synchronous=OFF")

			# Close
			cursor.close()
		except:
			 log.err()

	def _runInteraction(self, interaction, *args, **kwargs):
		connection = self.connectionFactory(self)
		transaction = self.transactionFactory(self, connection)

		try:
			result = (interaction(
				transaction,
				*args,
				**kwargs
			))

			connection.commit()

			# Success
			return result
		except:
			excType, excValue, excTraceback = sys.exc_info()

			try:
				connection.rollback()
			except:
				log.err(None, "Rollback failed")

			raise excType, excValue, excTraceback
		finally:
			transaction.close()

	def _runOperation(self, transaction, *a, **k):
		transaction.execute(*a, **k)

		# Success, return result with query params
		return dict(affected_rows=transaction.rowcount,
			insert_id=transaction.lastrowid)


sqlite = (SqliteConnectionPool(
	check_same_thread=False,
	database=config.get('db', 'database'),
	detect_types=sqlite3.PARSE_DECLTYPES
))
