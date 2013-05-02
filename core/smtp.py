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

from twisted.mail.smtp import ESMTPSenderFactory as _ESMTPSenderFactory, ESMTPSender as _ESMTPSender
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

	protocol = ESMTPSender
