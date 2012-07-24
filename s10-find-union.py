#!/usr/bin/env python
# MRwiki -- tools for analyzing Wikipedia content (especially links) using Python and MapReduce.
# Copyright (C) 2012 Sebastian Jaszczur, Maciej Kacprzak
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

jakisuuid = UUID('12345678901234567890123456789012')

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
        """
        id (typ, wart, ssid) ->
        -> ssid (typ, val, id)
        """
        if value[0] == "p" and len(value) == 3:
            if value[1] == "0":
                value = [value[0], value[2]]
            else:
                return
        id1 = key
        if len(value) < 3:
            value.append(ssid_gen(key))
        typ, wart, ssid = value
        yield ssid, (typ, wart, id1)
    
    def my_reduce_loop(self, key, values):
        """
        ssid (typ, wart, id) ->
        -> to samo
        """
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
                wart, id1 = value[1:]
                yield id1, [typ, wart, ssid]
    
    def steps(self):
        return ([self.mr(self.my_init_map, self.my_reduce_loop)]+
                [self.mr(self.my_mapper_nothing, self.my_reduce_loop)]*self.options.iterations+
                [self.mr(self.my_mapper_nothing, self.my_reduce_final)])

if __name__=="__main__":
    FindAndUnion.run()

# vim: ts=4 sw=4 et
