#!/usr/bin/env python
# MRwiki -- tools for analyzing Wikipedia content (especially links) using Python and MapReduce. 
# Copyright (C) 2007-2012  Lukasz Bolikowski
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

import mrjob.job, mrjob.protocol

class WeightFinder(mrjob.job.MRJob):
    """For each langlink calculates the number of pairs of pagelinks "supporting" the langlink.
    For example, for the langlink en:France <-> fr:France, the pair of pagelinks
    en:France -> en:Paris and fr:France -> fr:Paris "supports" the langlink,
    since there is a langlink en:Paris <-> fr:Paris.

    Langlinks are assumed to be undirected (symmetric), while the links of the other type are assumed to be directed."""

    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def pairType(self, pair):
        idA, idB = pair.split('#')
        langA = idA[0:idA.find(':')]
        langB = idB[0:idB.find(':')]
        return min(langA, langB) + '#' + max(langA, langB)

    def reducerA(self, key, values):
        langlinks, otherlinks = [], []
        for value in values:
            relType = value[0]
            toId = value[1]
            if relType == 'll':
                yield key, value
                langlinks.append(toId)
            else:
                otherlinks.append(toId)
        for otherlink in otherlinks:
            for langlink in langlinks:
                if langlink < key:
                    langPair = langlink + '#' + key
                    yield otherlink, ('suppRight', langPair)
                else:
                    langPair = key + '#' + langlink
                    yield otherlink, ('suppLeft', langPair)

    def reducerB(self, key, values):
        langs, lefts, rights = [], [], []
        for value in values:
            relType = value[0]
            toId = value[1]
            if relType == 'll':
                langs.append(toId)
            elif relType == 'suppLeft':
                lefts.append(toId)
            elif relType == 'suppRight':
                rights.append(toId)

        for lang in langs:
            if lang < key:
                pair = lang + '#' + key
            else:
                pair = key + '#' + lang
            pairType = self.pairType(pair)
            for left in lefts:
                if pairType == self.pairType(left):
                    yield left, ('suppLeft', pair)
            for right in rights:
                if pairType == self.pairType(right):
                    yield right, ('suppRight', pair)

    def reducerC(self, key, values):
        lefts, rights = set(), set()
        for value in values:
            if value[0] == 'suppLeft':
                lefts |= set([value[1]])
            if value[0] == 'suppRight':
                rights |= set([value[1]])
        common = len(lefts & rights)
        if common > 0:
            yield key, ('llSpl', common)

    def steps(self):
        return [self.mr(None, self.reducerA), self.mr(None, self.reducerB), self.mr(None, self.reducerC)]

if __name__ == '__main__':
    WeightFinder.run()

# vim: ts=4 sw=4 et
