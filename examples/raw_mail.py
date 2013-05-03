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
import socket

from json import dumps as jdumps, loads as jloads
from struct import pack, unpack

IP = '127.0.0.1'
PORT = 6132
DATA = ((
	( (dict(
		command='mail',
		message=dict(
			html='html',
			subject='subj',
			sender=dict(
				name='name',
				email='test@email.com'
			),
		),
		to=dict(name='name1', email='email1@email.com')
	)), ) * 200000
))

s = (socket.socket(
	socket.AF_INET,
	socket.SOCK_STREAM
))

try:
	s.connect((IP, PORT))

	a = s.sendall
	r = s.recv

	for d in DATA:
		d = jdumps(d)
		l = len(d)

		a(pack('!I', l))
		a(d)

		l = r(4)
		l = unpack('!I', l)[0]

		d = r(l)
		d = jloads(d)

		# Success
		print(d)
except socket.error, e:
	# Skip errors
	print(e)
finally:
	try:
		s.close()
	except socket.error, e:
		pass
