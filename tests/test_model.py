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

"""Unit tests of the data model."""

import mrwiki.model
import unittest

class TestModel(unittest.TestCase):

    def setUp(self):
        self.p = mrwiki.model.Page('en:42', 0, 'Foo')
        self.r = mrwiki.model.Redirect('fr:23', 2, 'Bolo1729')
        self.ll = mrwiki.model.Langlink('es:123', 'pl', 'Warszawa')
        self.pl = mrwiki.model.Pagelink('de:1234', 0, 'Warschau')

        self.p2 = mrwiki.model.Page('en:42', 0, 'Foo', 'en:123',
                                    'b4ff5c52ca6e686ddae17575decdd273a551bb1d',
                                    {'naive': 'e479ee0b551945239c102fcf054326127f0560ff'},
                                    {'simple': (2.0, -1.0)})

        self.r2 = mrwiki.model.Redirect('fr:23', 2, 'Bolo1729', 'fr:32')
        self.ll2 = mrwiki.model.Langlink('es:123', 'pl', 'Warszawa', 'pl:69', 5,
                                         '9ee141b1fc3cddc191c20fff22c43f46c20bd8bc',
                                         {'genetic': '4b88661602fe1b513de825479c98a3b3d3afa01d'})
        self.pl2 = mrwiki.model.Pagelink('de:1234', 0, 'Warschau', 'de:512')

    def test_simple_page(self):
        p = mrwiki.model.Relation.fromBasic(self.p.toBasic())
    
        self.assertEqual('en:42', p.fromId)
        self.assertEqual(0, p.namespace)
        self.assertEqual('Foo', p.title)
        self.assertEqual(None, p.redirect)
        self.assertEqual(None, p.component)
        self.assertEqual({}, p.meanings)
        self.assertEqual({}, p.positions)
    
    def test_complex_page(self):
        p2 = mrwiki.model.Relation.fromBasic(self.p2.toBasic())
        
        self.assertEqual('en:42', p2.fromId)
        self.assertEqual(0, p2.namespace)
        self.assertEqual('Foo', p2.title)
        self.assertEqual('en:123', p2.redirect)
        self.assertEqual('b4ff5c52ca6e686ddae17575decdd273a551bb1d', p2.component)
        self.assertEqual('e479ee0b551945239c102fcf054326127f0560ff', p2.meanings['naive'])
        self.assertEqual((2.0, -1.0), p2.positions['simple'])
    
    def test_simple_redirect(self):
        r = mrwiki.model.Relation.fromBasic(self.r.toBasic())
    
        self.assertEqual('fr:23', r.fromId)
        self.assertEqual(2, r.toNamespace)
        self.assertEqual('Bolo1729', r.toTitle)
        self.assertEqual(None, r.toId)
    
    def test_complex_redirect(self):
        r2 = mrwiki.model.Relation.fromBasic(self.r2.toBasic())
    
        self.assertEqual('fr:23', r2.fromId)
        self.assertEqual(2, r2.toNamespace)
        self.assertEqual('Bolo1729', r2.toTitle)
        self.assertEqual('fr:32', r2.toId)
    
    def test_simple_langlink(self):
        ll = mrwiki.model.Relation.fromBasic(self.ll.toBasic())
    
        self.assertEqual('es:123', ll.fromId)
        self.assertEqual('pl', ll.toLang)
        self.assertEqual('Warszawa', ll.toTitle)
        self.assertEqual(None, ll.toId)
    
    def test_complex_langlink(self):
        ll2 = mrwiki.model.Relation.fromBasic(self.ll2.toBasic())
    
        self.assertEqual('es:123', ll2.fromId)
        self.assertEqual('pl', ll2.toLang)
        self.assertEqual('Warszawa', ll2.toTitle)
        self.assertEqual('pl:69', ll2.toId)
        self.assertEqual(5, ll2.support)
        self.assertEqual('9ee141b1fc3cddc191c20fff22c43f46c20bd8bc', ll2.component)
        self.assertEqual('4b88661602fe1b513de825479c98a3b3d3afa01d', ll2.meanings['genetic'])
    
    def test_simple_pagelink(self):
        pl = mrwiki.model.Relation.fromBasic(self.pl.toBasic())
    
        self.assertEqual('de:1234', pl.fromId)
        self.assertEqual(0, pl.toNamespace)
        self.assertEqual('Warschau', pl.toTitle)
        self.assertEqual(None, pl.toId)
    
    def test_complex_pagelink(self):
        pl2 = mrwiki.model.Relation.fromBasic(self.pl2.toBasic())
    
        self.assertEqual('de:1234', pl2.fromId)
        self.assertEqual(0, pl2.toNamespace)
        self.assertEqual('Warschau', pl2.toTitle)
        self.assertEqual('de:512', pl2.toId)
    
    def test_fromLang(self):
        self.assertEqual('en', self.p.fromLang)
        self.assertEqual('fr', self.r.fromLang)
        self.assertEqual('es', self.ll.fromLang)
        self.assertEqual('de', self.pl.fromLang)
    
    def test_equality(self):
        def boilerplate(o):
            self.assertEqual(o, mrwiki.model.Relation.fromBasic(o.toBasic()))
        
        map(boilerplate, [self.p, self.r, self.ll, self.pl])
        map(boilerplate, [self.p2, self.r2, self.ll2, self.pl2])
        
        self.assertNotEqual(self.p, self.p2)
        self.assertNotEqual(self.r, self.r2)
        self.assertNotEqual(self.ll, self.ll2)
        self.assertNotEqual(self.pl, self.pl2)
