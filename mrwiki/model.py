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

class CommonEqualityMixin(object):
    """Allows field-by-field comparison of class instances."""

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        for field in self.__slots__:
            if getattr(self, field) != getattr(other, field):
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

class Relation(CommonEqualityMixin):
    """Base class for all the relations."""
    
    __slots__ = ['fromId', 'relType']

    def __init__(self, fromId, relType):
        self.fromId = fromId
        self.relType = relType

    @property
    def fromLang(self):
        return self.fromId[0:self.fromId.find(':')]

    def isPage(self):
        return self.relType == 'p'

    def isRedirect(self):
        return self.relType == 'r'

    def isLanglink(self):
        return self.relType == 'll'

    def isPagelink(self):
        return self.relType == 'pl'

    def toBasic(self):
        basicRepresentation = {}
        for field in self.__slots__:
            basicRepresentation[field] = getattr(self, field)
        return basicRepresentation

    @classmethod
    def fromBasic(self, basicRepresentation):
        relType = basicRepresentation['relType']
        if relType == 'p':
            o = Page(None, None, None)
        elif relType == 'r':
            o = Redirect(None, None, None)
        elif relType == 'll':
            o = Langlink(None, None, None)
        elif relType == 'pl':
            o = Pagelink(None, None, None)
        for field in basicRepresentation:
            setattr(o, field, basicRepresentation[field])
        return o

class Page(Relation):
    """Basic information about a page."""

    __slots__ = ['fromId', 'relType', 'namespace', 'title', 'redirect', 'component', 'meanings', 'positions']

    def __init__(self, fromId, namespace, title, redirect = None, component = None, meanings = None, positions = None):
        Relation.__init__(self, fromId, 'p')
        self.namespace = namespace
        self.title = title
        self.redirect = redirect
        self.component = component
        self.meanings = meanings
        if self.meanings is None:
            self.meanings = {}
        self.positions = positions
        if self.positions is None:
            self.positions = {}

class Redirect(Relation):
    """Redirect from one page to another page in the same language edition."""

    __slots__ = ['fromId', 'relType', 'toNamespace', 'toTitle', 'toId']

    def __init__(self, fromId, toNamespace, toTitle, toId = None):
        Relation.__init__(self, fromId, 'r')
        self.toNamespace = toNamespace
        self.toTitle = toTitle
        self.toId = toId

class Langlink(Relation):
    """Link from one page to another page in a different language edition."""

    __slots__ = ['fromId', 'relType', 'toLang', 'toTitle', 'toId', 'support', 'component', 'meanings']

    def __init__(self, fromId, toLang, toTitle, toId = None, support = None, component = None, meanings = None):
        Relation.__init__(self, fromId, 'll')
        self.toLang = toLang
        self.toTitle = toTitle
        self.toId = toId
        self.support = support
        self.component = component
        self.meanings = meanings
        if self.meanings is None:
            self.meanings = {}

    @property
    def signature(self):
        return min(self.fromId, self.toId) + '#' + max(self.fromId, self.toId)

class Pagelink(Relation):
    """Link from one page to another page in the same language edition."""

    __slots__ = ['fromId', 'relType', 'toNamespace', 'toTitle', 'toId']

    def __init__(self, fromId, toNamespace, toTitle, toId = None):
        Relation.__init__(self, fromId, 'pl')
        self.toNamespace = toNamespace
        self.toTitle = toTitle
        self.toId = toId

# vim: ts=4 sw=4 et
