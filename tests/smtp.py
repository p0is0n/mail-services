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

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '../'))

from twisted.application.service import Application, MultiService
from twisted.application.internet import TimerService, TCPServer
from twisted.internet import reactor
from twisted.internet.defer import DeferredQueue, inlineCallbacks, returnValue
from twisted.python.log import msg, err

from core.constants import DEBUG
from core.configs import config
from core.smtp import ESMTPSenderPool


application = Application("Mail-Services (tests)")

services = MultiService()
services.setServiceParent(application)

@inlineCallbacks
def _(*args):
	smtpPool = ESMTPSenderPool(poolsize=2, hostname='127.0.0.1', portname=25, username='test', password='')
	
	smtpPool.performRequest('sendMail', sender='test@localhost', to=('to@localhost', 'to2@localhost'), file='').addErrback(lambda *args: msg('a error', args)).addCallback(msg)
	smtpPool.performRequest('sendMail', sender='test@localhost', to=('to@localhost', 'to1@localhost'), file='').addErrback(lambda *args: msg('b error', args)).addCallback(msg)

	yield None

reactor.callLater(0.1, _)
