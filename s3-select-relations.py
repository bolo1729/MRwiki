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

class RelationsFilter(mrjob.job.MRJob):
    """Passes only relations of a given type (redirects, langlinks).
    Relation type is specified as a command line argument --rel-type."""

    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def configure_options(self):
        super(RelationsFilter, self).configure_options()
        self.add_passthrough_option('--rel-type', type='string', default=None, help='Type of relations to select')

    def mapper(self, key, line):
        if line[0] == self.options.rel_type:
            yield key, line

    def reducer(self, key, values):
        for value in values:
            yield key, value

if __name__ == '__main__':
    RelationsFilter.run()

# vim: ts=4 sw=4 et
