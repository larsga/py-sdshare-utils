"""
A simple SDShare server with pluggable backends based on web.py. The
code is very much prototype quality.
"""

import os, datetime, cgi, time, traceback, sys, csv
import web
from xml.sax import make_parser
from saxtracker import SAXTracker
 
urls = (
    '/', 'OverviewFeed',
    '/collection/(.+)', 'CollectionFeed',
    '/fragments/(.+)', 'FragmentsFeed',
    '/fragment/([^/]+)/(.+)', 'FragmentService',
    '/snapshots/(.+)', 'SnapshotsFeed',
    '/snapshot/(.+)', 'SnapshotService',
    )
 
PAGE_SIZE = 1000
BATCH_SIZE = 10000
 
# --- PAGES
 
class OverviewFeed:
    def GET(self):
        web.header("Content-Type", "application/atom+xml; charset=utf-8")
        return render.overview_feed(server)
 
class CollectionFeed:
    def GET(self, id):
        web.header("Content-Type", "application/atom+xml; charset=utf-8")
        coll = server.get_collection_by_id(id)
        return render.collection(coll)
 
class FragmentsFeed:
    def GET(self, id):
        web.header("Content-Type", "application/atom+xml; charset=utf-8")
        params = web.input(since = None)
        coll = server.get_collection_by_id(id)
        return render.fragments(coll, params.since)
 
class SnapshotsFeed:
    def GET(self, id):
        web.header("Content-Type", "application/atom+xml; charset=utf-8")
        coll = server.get_collection_by_id(id)
        return render.snapshots(coll)
 
class FragmentService:
    def GET(self, collid, fragid):
        web.header("Content-Type", "application/rdf+xml; charset=utf-8")
        coll = server.get_collection_by_id(collid)
        frag = coll.get_fragment_by_id(fragid)
        return frag.render()
 
class SnapshotService:
    def GET(self, id):
        web.header("Content-Type", "application/rdf+xml; charset=utf-8")
        web.header('Transfer-Encoding', 'chunked')       
        coll = server.get_collection_by_id(id)
        for chunk in coll.snapshot():
            yield chunk
 
# --- INTERNAL DATA MODEL
 
RDF_HEADER = '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n'
RDF_FOOTER = '</rdf:RDF>\n'
 
class Server:
 
    def __init__(self, title, author):
        self._title = title
        self._author = author
        self._collections = []
 
    def get_title(self):
        return self._title
 
    def get_author(self):
        return self._author
 
    def get_collections(self):
        return self._collections
 
    def add_collection(self, collection):
        self._collections.append(collection)
 
    def get_collection_by_id(self, id):
        for coll in self._collections:
            if coll.get_id() == id:
                return coll

    def get_timestamp(self):
        return now_timestamp()
            
    def set_author(self, author):
        self._author = author

    def set_title(self, title):
        self._title = title

    def get_guid(self):
        return "http://www.example.org/collections" # FIXME
            
class Collection:
 
    def __init__(self, title, id, uri, server):
        self._title = title
        self._id = id
        self._uri = uri
        self._server = server
        self._feeds = []
 
    def get_title(self):
        return self._title
 
    def get_id(self):
        return self._id

    def get_guid(self):
        return "http://example.org/collections/1/"

    def get_author(self):
        return self._server.get_author()
    
    def get_uri(self):
        return self._uri
 
    def get_feeds(self):
        return self._feeds
 
    def get_updated(self):
        return now_timestamp()
 
    def add_feed(self, feed):
        self._feeds.append(feed)
 
    def get_fragments(self, since):
        return self._feeds[0].get_fragments(since)
 
    def get_fragment_by_id(self, id):
        return self._feeds[0].get_fragment_by_id(id)
 
    def snapshot(self):
        for chunk in self._feeds[0].snapshot():
            yield chunk
 
class FragmentPage:
    """This class is used to wrap the list of fragments returned by
    the fragments feed, so that we can keep the paging logic in the
    Python code, rather than in the templates. It also helps us avoid
    having to duplicate it across different fragment feed
    implementations."""
 
    def __init__(self, fragments, since, lastid):
        self._fragments = fragments
        self._since = since
        self._lastid = lastid
 
    def get_fragments(self):
        return self._fragments[ : PAGE_SIZE]
 
    def has_next_page(self):
        return len(self._fragments) > PAGE_SIZE
 
    def get_params(self):
        "Produces the link parameters for the next page."
        return "since=" + self._since

class FragmentFeed:
    "Abstract fragment feed class to define the interface."

    def get_fragments(self, since):
        "since is a string."
        pass # returns a FragmentPage object
 
    def get_fragment_by_id(self, id):
        pass # returns a Fragment object
 
    def snapshot(self):
        pass # returns the actual snapshot in RDF/XML format

class Fragment:
    "Abstract fragment class, for reuse and to define interface."
 
    def __init__(self, id, updated, feed):
        self._id = id
        self._updated = updated
        self._feed = feed
 
    def get_id(self):
        return self._id
 
    def get_updated(self):
        return format_atom(self._updated)
 
    def get_syntax(self):
        return "application/rdf+xml"
 
    def get_uri(self):
        return self._feed.make_uri(self._id)
 
    def render(self):
        pass

# --- CSV backend

class CSVFragmentFeed(FragmentFeed):

    def __init__(self, source, type, pattern, timestampcol):
        self._source = source
        self._type = type
        self._pattern = pattern
        self._timestampcol = timestampcol
        self._columns = []
        self._loaded = False
        self._fragments = {} # uri -> fragment

    def get_fragments(self, since):
        if not self._loaded:
            self._load()
        
        # FIXME: must interpret 'since'
        subset = [f for f in self._fragments.values()
                  if (not since) or f.get_updated() >= since]
        lastid = None
        if subset:
            lastid = subset[-1].get_id()

        # FIXME: whoa! we can't do paging...
        return FragmentPage(subset, since, lastid)

    def get_type(self):
        return self._type
    
    def get_fragment_by_id(self, id):
        if not self._loaded:
            self._load()
        return self._fragments[id]
 
    def snapshot(self):
        if not self._loaded:
            self._load()
        all_decls = self._get_all_decls()
        yield RDF_HEADER
        yield render_ns_decls(all_decls) + '>\n'

        for f in self._fragments.values():
            resource = Resource(f.get_uri(), self._type)
            self._add_properties(resource, f)
            yield resource.render(all_decls)
        
        yield RDF_FOOTER

    def add_column(self, column):
        self._columns.append(column)

    def _get_all_decls(self):
        return extract_ns_decls([col.get_property() for col in self._columns])
        
    def _load(self):
        inf = open(self._source)
        reader = csv.reader(inf)
        headers = reader.next()

        for row in reader:
            obj = {}
            for ix in range(len(row)):
                value = row[ix].strip()
                if value:
                    obj[headers[ix]] = value

            uri = self._pattern % obj
            # FIXME: must interpret timestamp into comparable value
            frag = CSVFragment(uri, obj[self._timestampcol], self, obj)
            self._fragments[uri] = frag
        
        inf.close()
        self._loaded = True

    def _add_properties(self, resource, fragment):
        for col in self._columns:
            value = col.get_value(fragment.get_values())
            if value:
                resource.add_property(col.get_property(), value,
                                      col.is_literal())

def get_ns(prop):
    ix = prop.rfind('/')
    return prop[ : ix + 1]
 
def extract_ns_decls(properties):
    nses = {}
    for prop in properties:
        ns = get_ns(prop)
        if nses.has_key(ns):
            continue
        nses[ns] = "pre%s" % len(nses)
    return nses
 
def render_ns_decls(decls):
    return '\n'.join(['xmlns:%s="%s"' % (pre, ns) for (ns, pre) in decls.items()])
        
class Column:
 
    def __init__(self, name, prop, literal = True, uripattern = None):
                self._name = name
                self._prop = prop
                self._literal = literal
                self._uripattern = uripattern
 
    def get_property(self):
        return self._prop
 
    def get_value(self, obj):
        v = obj.get(self._name)
        if not v or not v.strip():
            return None
        if self._uripattern:
            return self._uripattern % v
        return v
 
    def is_literal(self):
        return self._literal

class CSVFragment(Fragment):

    def __init__(self, id, updated, feed, obj):
        Fragment.__init__(self, id, updated, feed)
        self._obj = obj

    def get_title(self):
        return "No title" # at least not yet
        
    def get_uri(self):
        return self._id

    def get_values(self):
        return self._obj
 
    def render(self):
        resource = Resource(self.get_uri(), self._feed.get_type())
        self._feed._add_properties(resource, self)
        decls = resource.get_ns_decls()
        return (RDF_HEADER + render_ns_decls(decls) + '>\n' +
                resource.render(decls) + RDF_FOOTER)

class Resource:
 
    def __init__(self, uri, type):
        self._uri = uri
        self._type = type
        self._properties = []
 
    def add_property(self, prop, value, literal):
        if value != None and value != "":
            self._properties.append((prop, value, literal))
 
    def render(self, decls):
        return ('  <rdf:Description rdf:about="%s">\n' +
                '    <rdf:type rdf:resource="%s"/>\n' +
                self._render_props(decls) + '\n' +
                '  </rdf:Description>\n') % (self._uri, self._type)
 
    def get_ns_decls(self):
        return extract_ns_decls([p for (p, v, l) in self._properties])
 
    def _render_props(self, decls):
        rendered = []
        for (prop, value, literal) in self._properties:
            ns = get_ns(prop)
            pre = decls[ns]
            local = prop[len(ns) : ]
 
            if literal:
                rendered.append("    <%s:%s>%s</%s:%s>" %
                                (pre, local, cgi.escape(value), pre, local))
            else:
                rendered.append("    <%s:%s rdf:resource='%s'/>" %
                                (pre, local, cgi.escape(value)))
 
        return "\n".join(rendered)

# --- RDBMS BACKEND

try:
    import psycopg2
except ImportError:
    # okay, it failed, so we can't connect to postgresql, but maybe we
    # don't need to, so let's deal with that later.
    pass

class SQLFragmentFeed:
 
    def __init__(self, uripattern, idcol, timecol, table, type, builder, filter = None):
        self._uripattern = uripattern
        self._idcol = idcol # FIXME: to be removed, but not right now
        self._timecol = timecol
        self._table = table
        self._type = type
        self._columns = []
        self._builder = builder
        self._filter = filter

    def get_fragments(self, since):
        "since is a string."
        pass # returns a FragmentPage object
 
    def get_fragment_by_id(self, id):
        pass # returns a Fragment object
 
    def snapshot(self):
        pass # returns the actual snapshot in RDF/XML format
    
# --- UTILITIES

def now_timestamp():
    return format_py(datetime.datetime.now())
    
def format_py(time):
    # 2008-07-17T15:47:17.062211Z
    return time.strftime("%Y-%m-%dT%H:%M:%SZ")
 
def format_atom(time):
    # 2008-07-17T15:47:17.062211Z
    return (str(time) + 'Z').replace(' ', 'T')
 
def parse_atom(timestr):
    assert timestr[-1] == 'Z'
    timestr = timestr[ : -1].replace('T', ' ')
    return Timestamp.valueOf(timestr)

# --- CONFIG LOADING

def set_param(obj, name, value):
    getattr(obj, "set_" + name)(value)

class ConfigHandler(SAXTracker):

    def __init__(self):
        SAXTracker.__init__(self, ["param"])
        self._server = Server(None, None)
        self._coll = None
        self._feed = None
        
        self._obj = self._server
        self._attrs = None
        # FIXME: for now, csv is the only backend type
        
    def startElement(self, name, attrs):
        SAXTracker.startElement(self, name, attrs)

        if name == "param":
            self._attrs = attrs

        elif name == "collection":
            self._coll = Collection(attrs["title"], attrs["id"], None,
                                    self._server)
            self._server.add_collection(self._coll)

        elif name == "relation":
            self._feed = CSVFragmentFeed(attrs["source"], attrs["type"],
                                         attrs["pattern"], attrs["timestamp"])
            self._coll.add_feed(self._feed)

        elif name == "property":
            self._feed.add_column(Column(attrs["column"], attrs["uri"], True))
        
    def endElement(self, name):
        SAXTracker.endElement(self, name)

        if name == "param":
            set_param(self._obj, self._attrs["name"], self._contents)

# --- INIT

handler = ConfigHandler()
p = make_parser()
p.setContentHandler(handler)
p.parse("config.xml")

server = handler._server

# --- STARTUP

web.config.debug = True
web.webapi.internalerror = web.debugerror
 
appdir = os.path.dirname(__file__)
render = web.template.render(os.path.join(appdir, 'templates/'))
app = web.application(urls, globals(), autoreload = False)
 
if __name__ == "__main__":
    app.run()
