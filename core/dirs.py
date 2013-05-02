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

from os.path import realpath, dirname, join

class Relative(str):

	def __init__(self, path):
		self.path = path

	def __call__(self, *a):
		return Relative(join(*(self.path, ) + a))

root = Relative(realpath(join(dirname(__file__), '..')))

logs = root('logs')
tmp = root('tmp')
tests = root('tests')
dbs = root('dbs')
