#!/usr/bin/env python
# MRwiki -- tools for analyzing Wikipedia content (especially links) using Python and MapReduce.
# Copyright (C) 2012  Maciej Kacprzak
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

class GraphBuilding(MRJob):
    INPUT_PROTOCOL = JSONProtocol
    def mapper(self, key, value):
        if value[0] == 'p':
            yield value[2], [key, value[1]]
        elif value[0] == 'r':
            yield value[2], [key]
    
    def reducer(self, key, values):
        slo = {}
        nie = {}
        for x in values:
            if len(x) == 2:
                lang = x[0].split(':',1)[0]
                if lang in slo:
                    slo[lang].append(x)
                else:
                    slo[lang] = [x]
            else:
                lang = x[0].split(':',1)[0]
                if lang in nie:
                    nie[lang].append(x[0])
                else:
                    nie[lang] = [x[0]]
        for l in slo:
            if l not in nie:
                nie[l] = []
            wynikowe = []
            for x in slo[l]:
                if x[0] not in nie[l]:
                    wynikowe.append(x[0])
            if len(wynikowe) > 1:
                yield (key,l), wynikowe

if __name__ == "__main__":
        GraphBuilding.run()

# vim: ts=4 sw=4 et
