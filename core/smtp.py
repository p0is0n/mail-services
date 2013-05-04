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

from twisted.python.log import msg, err
from twisted.internet import reactor
from twisted.internet import error

from twisted.mail.smtp import ESMTPSenderFactory as _ESMTPSenderFactory, ESMTPSender as _ESMTPSender, SMTPConnectError
from twisted.mail import relaymanager

from core.constants import DEBUG


class ESMTPSender(_ESMTPSender):

	debug = DEBUG

	def lineReceived(self, line):
		if DEBUG:
			msg('ESMTPSender', 'SMTP <<<', repr(line))

		# Success
		return _ESMTPSender.lineReceived(self, line)

	def sendLine(self, line):
		if DEBUG:
			msg('ESMTPSender', 'SMTP >>>', repr(line))

		# Success
		return _ESMTPSender.sendLine(self, line)


class ESMTPSenderFactory(_ESMTPSenderFactory):

	noisy = DEBUG
	protocol = ESMTPSender

	def _processConnectionError(self, connector, err):
		if self.retries < self.sendFinished <= 0:
			# Rewind the file in case part of it was read while attempting to send the message.
			self.file.seek(0, 0)

			# Try
			connector.connect()
			self.retries += 1
		elif self.sendFinished <= 0:
			if err.check(error.ConnectionDone):
				err.value = SMTPConnectError(-1, "Unable to connect to server.")

			self.result.errback(err.value)
