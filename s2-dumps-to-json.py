#!/usr/bin/env python
# Interwiki analysis tools
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

import mrjob.job, os, re, sys

class DumpsProcessor(mrjob.job.MRJob):
    def mapper(self, key, line):
        if not line.startswith('INSERT INTO'):
            return

        fileName = os.environ['map_input_file']
        match = re.match(r'(.*/)?(?P<lang>[a-z]+(_[a-z]+(_[a-z]+)?)?)wiki-(?P<date>\d{8})-(?P<type>[a-z]+).sql.gz', fileName)
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
        elif dumpType == 'categorylinks':
            for record in self.parseLine(line):
                yield self.processCategorylink(dumpLang, record)

    def reducer(self, key, values):
        for value in values:
            yield key, value

    def processPage(self, lang, record):
        pageId = record[0]
        namespace = record[1]
        title = record[2]

        self.increment_counter('processed', 'pages')
        key = '%s:%s' % (lang, pageId)
        return key, ('p', namespace, title.encode('utf-8'))

    def processRedirect(self, lang, record):
        fromId = record[0]
        toNamespace = record[1]
        toTitle = record[2]

        self.increment_counter('processed', 'redirects')
        key = '%s:%s' % (lang, fromId)
        return key, ('r', toNamespace, toTitle.encode('utf-8'))

    def processLanglink(self, fromLang, record):
        fromId = record[0]
        toLang = record[1].encode('utf-8')
        toTitle = record[2]

        self.increment_counter('processed', 'langlinks')
        key = '%s:%s' % (fromLang, fromId)
        return key, ('ll', toLang, toTitle.encode('utf-8'))

    def processPagelink(self, lang, record):
        fromId = record[0]
        toNamespace = record[1]
        toTitle = record[2]

        self.increment_counter('processed', 'pagelinks')
        key = '%s:%s' % (lang, fromId)
        return key, ('pl', toNamespace, toTitle.encode('utf-8'))

    def processCategorylink(self, lang, record):
        fromId = record[0]
        toTitle = record[1]

        self.increment_counter('processed', 'categorylinks')
        key = '%s:%s' % (lang, fromId)
        return key, ('cl', toTitle.encode('utf-8'))

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
                        raise Exception, 'Unexpected end of line'
                    
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
                            raise Exception, 'Unexpected end of line'
                        backslashes = 0
                        while line[cur - 1 - backslashes] == '\\':
                            backslashes += 1
                        if backslashes % 2 == 0:
                            break
                        cur += 1
                    
                    end = cur
                    cur += 1
                    if line[cur] != ',' and line[cur] != ')':
                        raise Exception
                    cur += 1
                    text = line[start:end].replace('\\\'', '\'').replace('\\\"', '\"').replace('\\\\', '\\').replace('_', ' ')
                    try:
                        text = text.decode('utf-8')
                    except UnicodeDecodeError:
                        valid = False
                    record += [text]
                    if line[end + 1] == ')':
                        break
            if valid:
                yield record

if __name__ == '__main__':
    DumpsProcessor.run()

# vim: ts=4 sw=4 et
