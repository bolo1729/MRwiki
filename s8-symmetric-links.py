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

class DirectionEraser(mrjob.job.MRJob):
    """Adds symmetric links."""

    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def mapper(self, key, line):
        fromId = key
        relType = line[0]
        toId = line[1]
        yield fromId, (relType, toId)
        yield toId, (relType, fromId)

    def reducer(self, key, values):
        targets = set()
        for value in values:
            targets |= set([(value[0], value[1])])
        for target in targets:
            yield key, target

if __name__ == '__main__':
    DirectionEraser.run()

# vim: ts=4 sw=4 et
