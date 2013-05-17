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
import math
import random

try:
	from cStringIO import StringIO
except ImportError:
	from StringIO import StringIO

from ConfigParser import ConfigParser, NoOptionError
from twisted.python import runtime
from twisted.python import log
from twisted.python.filepath import FilePath

if runtime.platform.supportsINotify():
	from twisted.internet.inotify import INotify, humanReadableMask, IN_MODIFY, IN_ISDIR

def rel(*x):
	return os.path.join(os.path.abspath(os.path.dirname(__file__) + '/../'), *x)


class Notify:
	"""Support for auto-reload changed files"""

	def notify(self, run=True):
		file = self.readed_file

		if not file:
			# Skip this
			return

		if runtime.platform.supportsINotify():
			# Use linux inotify API
			if run:

				def _notify(ignored, filepath, mask, file=file):
					if filepath.isfile() and file.endswith(filepath.basename()):
						log.msg(self, 'change', filepath,
							humanReadableMask(mask))

						# Reload file
						self.read(filepath.path)

				self._notifier = INotify()
				self._notifier.startReading()

				# Add watch
				self._notifier.watch(FilePath(file).parent(),
					mask=IN_MODIFY, callbacks=(_notify, ))

			else:
				# Stop watcher
				pass


class Config(ConfigParser, Notify):

	defaultConfig = '\n'.join((
	  '[common]',
	  'debug=no',
	  'bindAddresses=""',

	  '[db]',
	  'sync=3600 0, 120 100',

	  '[sender]',
	  'debug=no',
	  'workers=1',
	  'interval-empty=5.0',
	  'interval-next=0.5',
	  'attach-images=yes',
	  'attach-images-threads=5',

	  '[receiver]',
	  'listen=tcp:6132',

	  '[garbage]',
	  'messages-interval=600',
	  'old-last-messages=1800',
	  'old-time-messages=86400',

	  '[smtp]',
	  'hostname=localhost',
	  'portname=25',
	  'username=',
	  'password=',
	  'ssl=no',
	))

	readed_file = None

	def __init__(self, *a, **k):
		ConfigParser.__init__(self, *a, **k)

		# Default
		self.readfp(StringIO(self.defaultConfig))

		# Read configs
		for file in (rel('configs.conf'), ):
			if os.path.exists(file):
			  self.read(file)

			  # Stop
			  break

	def debugMode(self):
		return not not self.getboolean('common', 'debug')

	def dbSync(self):
		sync = []

		for row in self.get('db', 'sync').split(','):
			row = row.split()
			row = map(lambda row: int(row.strip()), row)

			if len(row) == 2:
				sync.append(row)

		# Success
		return sync


config = Config()

# Remove not usable
del Config, StringIO, ConfigParser, os, sys
