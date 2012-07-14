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

class DblRedirectCleaner(mrjob.job.MRJob):
    """Shortens or removes double redirects.
    When --shorten=1, double redirects A->B->C are shortened to A->C and B->C.
    When --shorten=0, double redirects A->B->C are removed, leaving only B->C.
    """

    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def configure_options(self):
        super(DblRedirectCleaner, self).configure_options()
        self.add_passthrough_option('--shorten', type='int', default=0, help='One means shorten double redirects, zero means remove them')

    def mapper(self, key, line):
        fromId = key
        relType = line[0]
        if relType <> 'r':
            return
        toId = line[1]
        yield fromId, ('r', toId)
        yield toId, ('rR', fromId)

    def reducer(self, key, values):
        toId, fromIds = None, []
        for value in values:
            relType = value[0]
            if relType == 'r':
                toId = value[1]
            elif relType == 'rR':
                fromIds.append(value[1])

        if toId is None:
            self.increment_counter('Progress', 'Passed', len(fromIds))
            for fromId in fromIds:
                yield fromId, ('r', key)
            return

        if toId is not None and self.options.shorten:
            for fromId in fromIds:
                if fromId == toId:
                    self.increment_counter('Progress', 'Removed circular')
                else:
                    self.increment_counter('Progress', 'Shortened')
                    yield fromId, ('r', toId)

        if toId is not None and not self.options.shorten:
            self.increment_counter('Progress', 'Removed', len(fromIds))

if __name__ == '__main__':
    DblRedirectCleaner.run()

# vim: ts=4 sw=4 et
