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

from twisted.application.service import Application, MultiService
from twisted.application.internet import TimerService, TCPServer
from twisted.python.log import msg, err

from services.sender import SenderService
from services.receiver import ReceiverService


application = Application("Mail-Services")

# gc.set_debug(gc.DEBUG_LEAK)
# gc.enable()

# Make services
senderService = SenderService()
receiverService = ReceiverService('tcp:58969')

services = MultiService()
services.setServiceParent(application)

services.addService(senderService)
services.addService(receiverService)
