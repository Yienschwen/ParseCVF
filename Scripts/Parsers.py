from collections import namedtuple
from html.parser import HTMLParser
from urllib.parse import urljoin

Thumbnail = namedtuple('Thumbnail', ['idx', 'title', 'link'])

class ThumbnailParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self._reset()
    
    def _reset(self, link=None):
        self._thumb_link = link
        self.titles = list()
        self._link = None
        self._title = None
        self._count = 0
    
    def handle_starttag(self, tag, attrs):
        if tag == 'dt' and len(attrs) == 1:
            if attrs[0] == ('class', 'ptitle'):
                self._link = True
        elif tag == 'a' and len(attrs) == 1 and self._link == True:
            if attrs[0][0] == 'href':
                self._link = attrs[0][1]
                self._title = True

    def handle_data(self, data):
        if self._title == True:
            self._title = data
            self.titles.append(Thumbnail(self._count, self._title, urljoin(self._thumb_link, self._link)))
            self._count += 1
            self._link = None
            self._title = None
    
    def feed(self, link, data):
        self._reset(link)
        HTMLParser.feed(self, data)
        return self.titles

Detail = namedtuple('Detail', ['authors', 'pdf', 'Supp', 'arXiv', 'abstract'])

class DetailParser(HTMLParser):
    _link_texts = ['pdf', 'Supp', 'arXiv']
    
    def __init__(self):
        HTMLParser.__init__(self)
        self._reset()
    
    def _reset(self, link=None):
        self._abs_link = link
        self._abstract = None
        self._links = [None] * 3
        self._href = None
        self._authors = list()
    
    def handle_starttag(self, tag, attrs):
        if tag == 'div' and len(attrs) == 1:
            if attrs[0] == ('id', 'abstract'):
                self._abstract = True
        elif tag == 'a' and len(attrs) == 1:
            if attrs[0][0] == 'href':
                self._href = attrs[0][1]
        elif tag == 'meta' and len(attrs) == 2:
            if attrs[0] == ('name','citation_author') and attrs[1][0] == 'content':
                self._authors.append(attrs[1][1])

    def handle_data(self, data):
        if self._abstract == True:
            self._abstract = data
        elif type(self._href) == str and data in DetailParser._link_texts:
            self._links[DetailParser._link_texts.index(data)] \
                = urljoin(self._abs_link, self._href) if data != 'arXiv' else self._href
            self._href = None
    
    def feed(self, link, data):
        self._reset(link)
        HTMLParser.feed(self, data)
        return Detail(self._authors, *(self._links), self._abstract)