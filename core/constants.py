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
import time

try:
	from collections import OrderedDict
except ImportError:
	OrderedDict = dict

from core.configs import config
from core.dirs import root


DEBUG = config.debugMode()
DEBUG_SENDER = config.getboolean('sender', 'debug')
DEBUG_DUMPS = False

CHARSET = 'utf8'

USERAGENT = 'Mozilla/5.0 (X11; Linux i686 on x86_64; rv:8.0.1) Gecko/20100101 Firefox/8.0.1'
DEFAULT_HTTP_TIMEOUT = 50

QUEUE_MAX_PRIORITY = 10
QUEUE_PAUSE_PRIORITY = QUEUE_MAX_PRIORITY * 2
QUEUE_UNPAUSE_PRIORITY = 1

(
	GROUP_STATUS_ACTIVE,
	GROUP_STATUS_PAUSED,
	GROUP_STATUS_INACTIVE,
) = range(1, 4)

GROUP_STATUSES = (dict(
	active=GROUP_STATUS_ACTIVE,
	paused=GROUP_STATUS_PAUSED,
	inactive=GROUP_STATUS_INACTIVE,
))

GROUP_STATUSES_NAMES = dict(((id, name) for name, id in GROUP_STATUSES.iteritems()))
