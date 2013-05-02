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

from twisted.python.log import msg, err
from twisted.internet.defer import Deferred
from twisted.internet.task import cooperate
from twisted.internet.reactor import callLater


def sleep(timeout, result=None):
	def canceller(deferred):
		caller.cancel()

	deferred = (Deferred(
		canceller=canceller))

	# Sleep with interval
	caller = callLater(timeout,
		deferred.callback, result)

	return deferred