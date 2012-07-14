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

class EffectiveLinksFinder(mrjob.job.MRJob):
    """Finds effective targets of links (after redirects)."""

    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def configure_options(self):
        super(EffectiveLinksFinder, self).configure_options()
        self.add_passthrough_option('--rel-type', type='string', default=None, help='Type of relations to process')

    def mapper(self, key, line):
        fromId = key
        relType = line[0]
        if relType == 'r':
            yield key, line
        else:
            toId = line[1]
            yield toId, (relType, fromId)

    def reducer(self, key, values):
        redirect, linksFrom = None, []
        for value in values:
            if value[0] == 'r':
                redirect = value[1]
            else:
                linksFrom.append(value[1])
        if redirect is None:
            for linkFrom in linksFrom:
                self.increment_counter('Processed', 'Passed')
                yield linkFrom, (self.options.rel_type, key)
        else:
            for linkFrom in linksFrom:
                self.increment_counter('Processed', 'Redirected')
                yield linkFrom, (self.options.rel_type, redirect)

if __name__ == '__main__':
    EffectiveLinksFinder.run()

# vim: ts=4 sw=4 et
