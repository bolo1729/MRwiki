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

import mrjob.job, mrwiki.model, os, re, sys

class Importer(mrjob.job.MRJob):
    """Parses dumps, converts them to relations and matches titles with page identifiers.
    
    Warning: for langlinks, only titles from the 0th namespace are matched."""

    OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

    def mapper(self, key, line):
        if not line.startswith('INSERT INTO'):
            return

        fileName = os.environ['map_input_file']
        match = re.match(r'(.*/)?(?P<lang>[a-z]+(_[a-z]+(_[a-z]+)?)?)wiki-(?P<date>\d{8})-(?P<type>[a-z]+)\.sql(\..*)?', fileName)
        if not match:
            return
        dumpLang = match.group('lang').replace('_', '-')
        dumpType = match.group('type')

        if dumpType == 'page':
            for record in self.parseLine(line):
                yield self.processPage(dumpLang, record)
        elif dumpType == 'redirect':
            for record in self.parseLine(line):
                yield self.processRedirect(dumpLang, record)
        elif dumpType == 'langlinks':
            for record in self.parseLine(line):
                yield self.processLanglink(dumpLang, record)
        elif dumpType == 'pagelinks':
            for record in self.parseLine(line):
                yield self.processPagelink(dumpLang, record)

    def reducer(self, qualTitle, relations):
        page = None
        rest = []
        for relation in relations:
            relation = mrwiki.model.Relation.fromBasic(relation)
            if relation.isPage():
                if page is not None:
                    self.increment_counter('Problems', 'Multiple pages with the same title')
                page = relation
            else:
                rest.append(relation)
        if page is None:
            self.increment_counter('Problems', 'Title with no matching page')
            return

        yield None, page.toBasic()
        for relation in rest:
            relation.toId = page.fromId
            yield None, relation.toBasic()

    def processPage(self, lang, record):
        pageId = record[0]
        namespace = record[1]
        title = record[2]

        self.increment_counter('Processed', 'Pages')
        qualTitle = '%s:%s:%s' % (lang, namespace, title)
        fromId = '%s:%s' % (lang, pageId)
        return qualTitle, mrwiki.model.Page(fromId, namespace, title).toBasic()

    def processRedirect(self, lang, record):
        fromId = record[0]
        toNamespace = record[1]
        toTitle = record[2]

        self.increment_counter('Processed', 'Redirects')
        qualTitle = '%s:%s:%s' % (lang, toNamespace, toTitle)
        fromId = '%s:%s' % (lang, fromId)
        return qualTitle, mrwiki.model.Redirect(fromId, toNamespace, toTitle).toBasic()

    def inferNamespace(self, lang, title):
        # TODO: really infer namespace
        return 0

    def processLanglink(self, fromLang, record):
        fromId = record[0]
        toLang = record[1].encode('utf-8')
        toTitle = record[2]
        toNamespace = self.inferNamespace(toLang, toTitle)

        self.increment_counter('Processed', 'Langlinks')
        qualTitle = '%s:%s:%s' % (toLang, toNamespace, toTitle)
        fromId = '%s:%s' % (fromLang, fromId)
        return qualTitle, mrwiki.model.Langlink(fromId, toLang, toTitle).toBasic()

    def processPagelink(self, lang, record):
        fromId = record[0]
        toNamespace = record[1]
        toTitle = record[2]

        self.increment_counter('Processed', 'Pagelinks')
        qualTitle = '%s:%s:%s' % (lang, toNamespace, toTitle)
        fromId = '%s:%s' % (lang, fromId)
        return qualTitle, mrwiki.model.Pagelink(fromId, toNamespace, toTitle).toBasic()

    def parseLine(self, line):
        cur = 0
        records = []
        while True:
            (record, valid) = ([], True)
            cur = line.find('(', cur)
            if cur == -1:
                break
            cur += 1

            while True:
                type = None
                if line[cur] == '\'':
                    type = '\''
                if line[cur] == '"':
                    type = '"'

                if not type:
                    start = cur
                    nextComma = line.find(',', cur)
                    if nextComma == -1:
                        nextComma = sys.maxint
                    nextBracket = line.find(')', cur)
                    if nextBracket == -1:
                        nextBracket = sys.maxint
                    end = min(nextComma, nextBracket)
                    if end == sys.maxint:
                        self.increment_counter('Problems', 'Unexpected EOL')
                        return
                    
                    record += [line[start:end].replace('_', ' ')]
                    cur = end + 1
                    if line[cur - 1] == ')':
                        break
                else:
                    cur += 1
                    start = cur
                    while True:
                        cur = line.find(type, cur)
                        if cur == -1:
                            self.increment_counter('Problems', 'Unexpected EOL redux')
                            return
                        backslashes = 0
                        while line[cur - 1 - backslashes] == '\\':
                            backslashes += 1
                        if backslashes % 2 == 0:
                            break
                        cur += 1
                    
                    end = cur
                    cur += 1
                    if line[cur] != ',' and line[cur] != ')':
                        self.increment_counter('Problems', 'Syntax error')
                        return
                    cur += 1
                    text = line[start:end].replace('\\\'', '\'').replace('\\\"', '\"').replace('\\\\', '\\').replace('_', ' ')
                    try:
                        text = text.decode('utf-8')
                    except UnicodeDecodeError:
                        self.increment_counter('Problems', 'Unicode decode error')
                        valid = False
                    # record += [text.encode('utf-8')]
                    record += [text]
                    if line[end + 1] == ')':
                        break
            if valid:
                yield record

if __name__ == '__main__':
    Importer.run()

# vim: ts=4 sw=4 et
