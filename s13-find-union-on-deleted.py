#!/usr/bin/env python
# MRwiki -- tools for analyzing Wikipedia content (especially links) using Python and MapReduce.
# Copyright (C) 2012 Sebastian Jaszczur
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from uuid import uuid3, UUID

jakisuuid = UUID('09876543210987654321098765432109')#it's other than in s10
#jakisuuid = UUID('12345678901234567890123456789012')

def ssid_gen(pid):
	if type(pid)!=type(str()):
		raise TypeError("String is required")
	return str(uuid3(jakisuuid, pid))

class FindAndUnion(MRJob):
	INPUT_PROTOCOL = JSONProtocol

	def configure_options(self):
		super(FindAndUnion, self).configure_options()
		self.add_passthrough_option('--iterations', type='int', default=5, help="Number of steps/iterations")

	def my_init_map(self, key, value):
		if key.count(":")>0:
			id1 = key
			typ, id2, ssid, waga = value
			yield (id1, id2), (typ, ssid, waga)
		else:
			ssid = key
			for x in value:
				typ, id1, id2, waga = x
				yield (id1, id2), ("dll", ssid, waga)
				yield (id2, id1), ("dll", ssid, waga)
	
	def my_init_reduce(self, key, values):
		normal = None
		deleted = False
		id1, id2 = key
		for value in values:
			typ, ssid, waga = value
			if typ == "ll":
				normal = value
			elif typ == "dll":
				deleted = True
			else:
				raise TypeError("Bad type?")
		if normal and not deleted:
			typ, ssid, waga = normal
			yield ssid_gen(id1), (typ, id2, id1, ssid, waga)
		elif normal and deleted:
			typ, ssid, waga = normal
			yield "deleted", ("dll", id2, id1, ssid, waga)

	def my_reduce_loop(self, key, values):
		"""
		ssid (typ, wart, id) ->
		-> to samo
		"""
		if key == "deleted":
			for value in values:
				yield key, value
			return
		tab = []
		ssids = set()
		for value in values:
			typ = value[0]
			if typ == "u":
				valssid = value[1]
				ssids.add(valssid)
			else:
				tab.append(value)
				id1 = value[2]
				if typ == "p":
					ssids.add(ssid_gen(id1))
				elif typ in ["r", "ll"]:
					id2 = value[1]
					ssids.add(ssid_gen(id1))
					ssids.add(ssid_gen(id2))
					
		mssid = min(ssids)
		if key != mssid:
			self.increment_counter("ReducerLoop", "ModSpojna")
		ssids.remove(mssid)
		for value in tab:
			yield mssid, value
		for ssid in ssids:
			yield ssid, ["u", mssid]

	def my_mapper_nothing(self, key, value):
		yield key, value

	def my_reduce_final(self, key, values):
		ssid = key
		for value in values:
			typ = value[0]
			if typ != "u":
				wart, id1, ssidg, waga = value[1:]
				yield id1, [typ, wart, ssid, ssidg, waga]

	def steps(self):
		return ([self.mr(self.my_init_map, self.my_init_reduce)]+
				[self.mr(self.my_mapper_nothing, self.my_reduce_loop)]*self.options.iterations+
				[self.mr(self.my_mapper_nothing, self.my_reduce_final)])

if __name__=="__main__":
    FindAndUnion.run()

# vim: ts=4 sw=4 et
