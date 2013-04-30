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
import types

from twisted.internet.defer import Deferred, DeferredLock, DeferredList, DeferredSemaphore
from twisted.internet.defer import DeferredQueue, inlineCallbacks, returnValue
from twisted.internet.defer import fail, succeed
from twisted.python.log import msg, err, startLogging
from twisted.internet import reactor

from core.db import sqlite
from core.dirs import tmp
from core.constants import DEBUG, CHARSET

startLogging(sys.stdout)

# Install callable
@inlineCallbacks
def _(*args):
	# Remove tables
	deferreds = []
	deferreds.append(sqlite.runOperation("""DROP TABLE IF EXISTS queue_messages"""))
	deferreds.append(sqlite.runOperation("""DROP TABLE IF EXISTS queue_to"""))
	deferreds.append(sqlite.runOperation("""DROP TABLE IF EXISTS queue_groups"""))

	msg((yield DeferredList(deferreds)))

	# Create tables
	msg((yield sqlite.runOperation("""
		CREATE TABLE queue_messages(
			subject TEXT NOT NULL,
			text TEXT,
			html TEXT,
			create_time INTEGER NOT NULL,
			params TEXT NOT NULL
		)
	""")))

	msg((yield sqlite.runOperation("""CREATE INDEX create_time_to_m ON queue_messages (create_time ASC)""")))

	msg((yield sqlite.runOperation("""
		CREATE TABLE queue_to(
			message_id INTEGER NOT NULL,
			group_id INTEGER,
			email TEXT NOT NULL,
			name TEXT NOT NULL,
			create_time INTEGER NOT NULL,
			parts TEXT NOT NULL,
			priority INTEGER NOT NULL DEFAULT 0
		)
	""")))

	msg((yield sqlite.runOperation("""CREATE INDEX message_to_i ON queue_to (message_id ASC)""")))
	msg((yield sqlite.runOperation("""CREATE INDEX group_to_i ON queue_to (group_id ASC)""")))
	msg((yield sqlite.runOperation("""CREATE INDEX create_time_to_i ON queue_to (create_time ASC)""")))

	msg((yield sqlite.runOperation("""
		CREATE TABLE queue_groups(
			id INTEGER PRIMARY KEY ASC,
			wait INTEGER NOT NULL DEFAULT 0,
			sent INTEGER NOT NULL DEFAULT 0,
			errors INTEGER NOT NULL DEFAULT 0
		)
	""")))

	# Success
	reactor.stop()

reactor.callLater(0.1, _, None)
reactor.run()
