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

class LinkChecker(mrjob.job.MRJob):
    """Performs various sanity checks on links.
    Removes any non-redirect links originating from redirect pages.
    Removes langlinks between pages in the same language.
    """

    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def mapper(self, key, line):
        fromId = key
        relType = line[0]
        toId = line[1]

        if relType == 'll':
            fromLang = fromId[0:fromId.find(':')]
            toLang = toId[0:toId.find(':')]
            if fromLang == toLang:
                self.increment_counter('Problems', 'Langlinks between pages in the same language')
            else:
                yield key, line
        else:
            yield key, line

    def reducer(self, key, values):
        redirect, others = None, []
        for value in values:
            if value[0] == 'r':
                redirect = value
            else:
                others.append(value)

        if redirect is None:
            for other in others:
                yield key, other
        else:
            if len(others) > 0:
                self.increment_counter('Problems', 'Links from redirect pages')
            yield key, redirect

if __name__ == '__main__':
    LinkChecker.run()

# vim: ts=4 sw=4 et
