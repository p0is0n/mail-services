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
import random
import time

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '../'))

from pprint import pprint
from twisted.trial.unittest import SynchronousTestCase
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from core.dirs import tmp
from core.utils import sleep
from core.mappers import Message


class MappersTest(SynchronousTestCase):

	def test_message_01(self):
		self.assertEquals(Message.fromDict(dict(id=1)).id, 1)


testCases = [MappersTest]
