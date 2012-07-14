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

class LinkMatcher(mrjob.job.MRJob):
    """Finds page identifiers of link targets."""

    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def configure_options(self):
        super(LinkMatcher, self).configure_options()
        self.add_passthrough_option('--rel-type', type='string', default=None, help='Type of relations to match')

    def mapper(self, key, line):
        pageId = key
        pageLang = key[0:key.find(':')]
        relType = line[0]
        if relType == 'p':
            pageNamespace = line[1]
            pageTitle = line[2]
            if self.options.rel_type == 'cl':
                if pageNamespace == '14':
                    yield pageLang + ':' + pageTitle, (relType + 'R', pageId)
            elif pageNamespace == '0':
                yield pageLang + ':' + pageTitle, (relType + 'R', pageId)
        elif relType == self.options.rel_type:
            if relType in ('r', 'pl'):
                if line[1] == '0':
                    yield pageLang + ':' + line[2], (relType + 'R', pageId)
            if relType == 'll':
                yield line[1] + ':' + line[2], (relType + 'R', pageId)
            if relType == 'cl':
                yield pageLang + ':' + line[1], (relType + 'R', pageId)

    def reducer(self, key, values):
        page, linksFrom = None, []
        for value in values:
            if value[0] == 'pR':
                page = value[1]
            else:
                linksFrom.append(value[1])
        if page is None:
            return
        for linkFrom in linksFrom:
            yield linkFrom, (self.options.rel_type, page)

if __name__ == '__main__':
    LinkMatcher.run()

# vim: ts=4 sw=4 et
