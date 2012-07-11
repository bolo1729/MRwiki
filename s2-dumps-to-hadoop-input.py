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

import gzip, logging, os, re, sys

class DumpsProcessor:
	def __init__(self, dataSource = None):
		self.log = logging.getLogger('Importer')
		self.dataSource = dataSource

	def doProcess(self):
		langs = self.dataSource.getLangs()
		if len(langs) > 0:
			self.log.info('Processing %d language(s): %s' % (len(langs), ' '.join(langs)))
		else:
			self.log.error('No languages found')
			return

		for lang in langs:
			self.log.info('Processing pages from ' + lang)
			self.dataSource.importTable(lang, 'page', lambda r : self.processPages(lang, r))

		for lang in langs:
			self.log.info('Processing redirects from ' + lang)
			self.dataSource.importTable(lang, 'redirect', lambda r : self.processRedirects(lang, r))

		for lang in langs:
			self.log.info('Processing langlinks from ' + lang)
			self.dataSource.importTable(lang, 'langlinks', lambda r : self.processLanglinks(lang, r))

#		for lang in langs:
#			self.log.info('Processing categorylinks from ' + lang)
#			self.dataSource.importTable(lang, 'categorylinks', lambda r : self.processCategorylinks(lang, r))

#		for lang in langs:
#			self.log.info('Processing pagelinks from ' + lang)
#			self.dataSource.importTable(lang, 'pagelinks', lambda r : self.processPagelinks(lang, r))

		self.log.info('Done')

	def processPages(self, lang, records):
		for record in records:
			pageId = record[0]
			namespace = record[1]
			title = record[2]

			print '%s:%s\tp\t%s\t%s' % (lang, pageId, namespace, title.encode('utf-8'))

	def processRedirects(self, lang, records):
		for record in records:
			fromId = record[0]
			toNamespace = record[1]
			toTitle = record[2]

			print '%s:%s\tr\t%s\t%s' % (lang, fromId, toNamespace, toTitle.encode('utf-8'))

	def processLanglinks(self, fromLang, records):
		for record in records:
			fromId = record[0]
			toLang = record[1].encode('utf-8')
			toTitle = record[2]

			print '%s:%s\tl\t%s:%s' % (fromLang, fromId, toLang, toTitle.encode('utf-8'))

	def processPagelinks(self, lang, records):
		for record in records:
			fromId = record[0]
			toNamespace = record[1]
			toTitle = record[2]

			pass

	def processCategorylinks(self, lang, records):
		for record in records:
			fromId = record[0]
			toTitle = record[1]

			pass

class DummyDataSource:
	def getLangs(self):
		return []
	def importTable(self, lang, type, callback):
		pass

class DumpsDataSource:
	def __init__(self, dumpsDir = None):
		self.dumpsDir = dumpsDir

	def getLangs(self):
		langs = []
		for filename in os.listdir(self.dumpsDir):
			match = re.match(r'(?P<lang>[a-z]+(_[a-z]+(_[a-z]+)?)?)wiki-\d{8}-page.sql.gz', filename)
			if not match:
				continue
			langs += [match.group('lang').replace('_', '-')]
		return sorted(langs)

	def importTable(self, lang, type, callback):
		source = None
		for filename in os.listdir(self.dumpsDir):
			match = re.match(r'(?P<lang>[a-z]+(_[a-z]+(_[a-z]+)?)?)wiki-(?P<date>\d{8})-(?P<type>[a-z]+).sql.gz', filename)
			if not match or lang != match.group('lang').replace('_', '-') or type != match.group('type'):
				continue
			source = gzip.open(self.dumpsDir + os.sep + filename)
			break
		if not source:
			raise Exception, 'No such file'
		for line in source:
			if not line.startswith('INSERT INTO'):
				continue
			self.importLine(line, callback)
		source.close()

	def importLine(self, line, callback):
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
				records += [record]
		callback(records)

if __name__ == '__main__':
	import logging.config

	logging.config.fileConfig(os.path.dirname(sys.argv[0]) + os.sep + 'logging.conf')

	dataSource = DumpsDataSource(sys.argv[1])
	processor = DumpsProcessor(dataSource)
	processor.doProcess()

