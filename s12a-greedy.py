#!/usr/bin/env python
# MRwiki -- tools for analyzing Wikipedia content (especially links) using Python and MapReduce.
# Copyright (C) 2012  Sebastian Jaszczur
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
from random import sample

ILOSC_WATKOW = 10

def lang(id0):
	return id0.split(":")[0]

class Zachlan(object):
	def __init__(self):
		self._nalezy_do = dict()
		self._grupa_ma_jezyki = dict()
		self._wynik = list()
	
	def wynik(self):
		return self._wynik
	
	def jest_w(self, x):
		if self._nalezy_do[x]==x:
			return x
		else:
			self._nalezy_do[x] = self.jest_w(self._nalezy_do[x])
			return self._nalezy_do[x]
	
	def zespol_grupy(self, id1, id2):
		if self.jest_w(id1) == self.jest_w(id2):
			return 0
		minid, maxid = sorted([self.jest_w(id1), self.jest_w(id2)])
		if self._grupa_ma_jezyki[minid] & self._grupa_ma_jezyki[maxid]:#to jest czesc wspolna tych grup
			return 1
		else:
			self._grupa_ma_jezyki[minid].update(self._grupa_ma_jezyki[maxid])
			self._nalezy_do[maxid]=minid
			return 0
	
	def dodaj(self, krawedz):
		typ, id1, id2, waga = krawedz
		for idi in [id1, id2]:
			if idi not in self._nalezy_do:
				self._nalezy_do[idi] = idi
				self._grupa_ma_jezyki[idi] = set([lang(idi)])
				
		if self.zespol_grupy(id1, id2):
			self._wynik.append(tuple(krawedz))

class Heurez(MRJob):
	INPUT_PROTOCOL = JSONProtocol
	
	def my_init_map(self, key, value):
		self.increment_counter("my_init_map", "my_init_map")
		id1 = key
		typ, id2, ssid, waga = value
		if typ != "ll":
			return
		else:
			for watek in xrange(ILOSC_WATKOW):
				yield (ssid, watek), [typ, id1, id2, waga]
		
	def my_reduce(self, key, values):
		self.increment_counter("my_reduce", "my_reduce")
		tab = []
		for value in values:
			tab.append(value)
		if key[1] == 0:
			tab.sort(lambda a, b: a[-1].__cmp__(b[-1]))
			tab.reverse()
		else:
			tab = sample(tab, len(tab))
		
		wynik = Zachlan()
		for x in tab:
			wynik.dodaj(x)
		
		yield key[0], wynik.wynik()
	
	def my_final_map(self, key, value):
		self.increment_counter("my_final_map", "my_final_map")
		yield key, value
	
	def my_final_reduce(self, key, values):
		self.increment_counter("my_final_reduce", "my_final_reduce")
		wynik = None
		ocena_wynik = None
		for value in values:
			ocena_val = sum([x[-1] for x in value])
			if wynik == None:
				wynik = value
				ocena_wynik = ocena_val
			elif ocena_wynik > ocena_val:
				wynik = value
				ocena_wynik = ocena_val
		yield key, wynik
	
	def steps(self):
		return ([self.mr(self.my_init_map, self.my_reduce)] +
			 	[self.mr(self.my_final_map, self.my_final_reduce)])


if __name__ == "__main__":
	Heurez.run()
