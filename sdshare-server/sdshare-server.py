# coding=utf-8

from java.text import SimpleDateFormat
from java.sql import Timestamp, SQLException
from com.ibm.db2.jcc.c import SqlException
import os, datetime, cgi, time, traceback
import web
 
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
        return render.overview_feed(server)
 
class CollectionFeed:
    def GET(self, id):
        coll = server.get_collection_by_id(id)
        return render.collection(coll)
 
class FragmentsFeed:
    def GET(self, id):
        params = web.input(since = None)
        coll = server.get_collection_by_id(id)
        return render.fragments(coll, params.since)
 
class SnapshotsFeed:
    def GET(self, id):
        coll = server.get_collection_by_id(id)
        return render.snapshots(coll)
 
class FragmentService:
    def GET(self, collid, fragid):
        coll = server.get_collection_by_id(collid)
        frag = coll.get_fragment_by_id(fragid)
        return frag.render()
 
class SnapshotService:
    def GET(self, id):
        web.header('Transfer-Encoding', 'chunked')       
        coll = server.get_collection_by_id(id)
        for chunk in coll.snapshot():
            yield chunk
 
# --- DATA MODEL
 
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
 
class Collection:
 
    def __init__(self, title, id, uri):
        self._title = title
        self._id = id
        self._uri = uri
        self._feeds = []
 
    def get_title(self):
        return self._title
 
    def get_id(self):
        return self._id
 
    def get_uri(self):
        return self._uri
 
    def get_feeds(self):
        return self._feeds
 
    def get_updated(self):
        return format_py(datetime.datetime.now())
 
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
        where = []
        date = None # don't really need this
        if since:
            date = parse_atom(since)
            where.append("%s >= %s" % \
                (self._timecol, self._builder.make_timestamp(date)))
        if self._filter:
            where.append(self._filter)
        where = "where " + " and ".join(where)
 
        # (1) open JDBC connection
        stmt = dba.create_statement()
        lastsince = None
        lastid = None
 
        # (2) do SQL query & build result
        fragments = []
        cutoff = self._builder.make_cutoff(PAGE_SIZE + 1)
        query = ("select %s, %s from %s %s order by %s asc, %s asc %s" %
                     (self._idcol, self._timecol, self._table, where,
                      self._timecol, self._idcol, cutoff))
        stmt.execute(query)
        rs = stmt.getResultSet()
        while rs.next():
            lastsince = rs.getTimestamp(self._timecol)
            lastid = rs.getString(self._idcol)
            fragments.append(Fragment(lastid, lastsince, self))                               
 
        # (3) return
        rs.close()
        stmt.close()
 
        lastsince = lastsince and format_atom(lastsince) # if none it stays none
        return FragmentPage(fragments, lastsince, lastid)
 
    def get_fragment_by_id(self, id):
        return Fragment(id, None, self)
 
    def get_type(self):
        return self._type
 
    def add_column(self, col):
        self._columns.append(col)
 
    def add_properties(self, rs, resource):
        for col in self._columns:
            value = col.get_value(rs)
            if value:
                resource.add_property(col.get_property(), value, col.is_literal())
 
    def make_uri(self, rs):
        if type(self._idcol) == str:
            return self._uripattern % rs.getString(self._idcol)
        else:
            return self._uripattern % ResultSetMap(rs)
 
    def snapshot(self):
        # have to do this in batches, to avoid running out of CPU time
        idtracker = IDColumnTracker(self._idcol)
        stmt = dba.create_statement()
        limitpart = self._builder.make_cutoff(BATCH_SIZE)
        t = time.time()
 
        all_decls = self.get_all_decls()
        yield RDF_HEADER
        yield render_ns_decls(all_decls) + '>\n'
 
        # loop over batches
        batch_no = 0
        while True:
            try:
                count = 0
                where = []
                if idtracker.has_previous_record():
                    where.append(idtracker.make_previous_condition())
                if self._filter:
                    where.append(self._filter)
                if where:
                    where = "where " + " and ".join(where)
                else:
                    where = ""
                sql = ("select * from %s %s order by %s %s" %
                            (self._table, where,
                             idtracker.get_column_list(),
                             limitpart))                
                stmt.execute(sql)
                rs = stmt.getResultSet()
 
                # render the batches
                while rs.next():
                    resource = Resource(self.make_uri(rs), self.get_type())
                    self.add_properties(rs, resource)
                    yield resource.render(all_decls)
                    count += 1
                    idtracker.record_position(rs)
 
                # are we done?
                print "%s resources" % (batch_no * BATCH_SIZE + count)
                batch_no += 1
                rs.close()
                
                if count < BATCH_SIZE:
                    break # yes
            except SqlException, e:
                stmt.reconnect(e) # let's try one more time               
 
        stmt.close()
        yield RDF_FOOTER
 
        print "TOTAL TIME:", time.time() - t
 
    def get_all_decls(self):
        return extract_ns_decls([col.get_property() for col in self._columns])

class IDColumnTracker:

    def __init__(self, idcols):
        if type(idcols) == str:
            self._idcols = (idcols, )
        else:
            self._idcols = idcols

        self._prev = None
        
    def get_column_list(self):
        return ", ".join(self._idcols)

    def has_previous_record(self):
        return self._prev is not None

    def make_previous_condition(self):
        return self._make_previous_condition(self._idcols, self._prev)
    
    def _make_previous_condition(self, idcols, prev):
        try:
            col = idcols[0]
            v = prev[0]

            part = "%s > %s" % (col, sql(v))
            
            if len(idcols) > 1:
                eq = " or (%s = %s " % (col, sql(v))
                sub = self._make_previous_condition(idcols[1 : ], prev[1 : ])
                return part + eq + " and (" + sub + "))"
            else:
                return part
        except:
            traceback.print_exc()
            return ""

    def record_position(self, rs):
        self._prev = [get_value(rs, idcol) for idcol in self._idcols]

def get_value(rs, idcol):
    return rs.getObject(idcol)
        
def sql(v):
    if type(v) == int:
        return v
    else:
        return "'%s'" % v
        
class Fragment:
 
    def __init__(self, id, updated, feed):
        self._id = id
        self._updated = updated
        self._feed = feed
 
    def get_id(self):
        return self._id
 
    def get_updated(self):
        return format_atom(self._updated)
 
    def get_syntax(self):
        return "application/xml+rdf"
 
    def get_uri(self):
        return self._feed.make_uri(self._id)
 
    def render(self):
        resource = Resource(self.get_uri(), self._feed.get_type())
 
        # run SQL query
        stmt = dba.create_statement()
        stmt.execute("select * from %s where %s = %s" %
                     (self._feed._table, self._feed._idcol, self._id))
        rs = stmt.getResultSet()
        if rs.next():
            # map to properties
            self._feed.add_properties(rs, resource)
 
        rs.close()
        stmt.close()
 
        # return RDF/XML
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
 
class Column:
 
    def __init__(self, name, prop, literal = True, uripattern = None):
                self._name = name
                self._prop = prop
                self._literal = literal
                self._uripattern = uripattern
 
    def get_property(self):
        return self._prop
 
    def get_value(self, rs):
        try:
            v = rs.getString(self._name)
        except SQLException, e:
            print e
            raise e
 
        if not v or not v.strip():
            return None
        if self._uripattern:
            return self._uripattern % v
        return v
 
    def is_literal(self):
        return self._literal
 
class StaticColumn:
 
    def __init__(self, prop, value, literal = True):
        self._prop = prop
        self._value = value
        self._literal = literal
 
    def get_property(self):
        return self._prop
 
    def get_value(self, rs):
        return value
 
    def is_literal(self):
        return self._literal
 
class FormatColumn:
 
    def __init__(self, prop, urlpattern):
        self._prop = prop
        self._urlpattern = urlpattern
 
    def get_property(self):
        return self._prop
 
    def get_value(self, rs):
        return self._urlpattern % ResultSetMap(rs)
 
    def is_literal(self):
        return False
 
class PgSQLBuilder:
    pgsqlformat = SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss'Z'")
 
    def make_timestamp(self, date):
        return "timestamp '%s'" % PgSQLBuilder.pgsqlformat.format(date)
 
    def make_cutoff(self, items):
        return "limit %s" % items
 
    def make_offset(self, offset):
        if offset:
            return "offset %s" % offset
        else:
            return ""
 
class DB2SQLBuilder:
 
    def make_timestamp(self, date):
        # "yyyy-MM-dd-HH:mm:ss.SSS" (fraction can be fairly detailed)
        datestr = str(date).replace(' ', '-') # using JDBC timestamp escape format
        return "timestamp('%s')" % datestr
 
    def make_cutoff(self, items):
        return "fetch first %s rows only" % items
 
    def make_offset(self, offset):
                if not offset:
                        return ""
                raise Exception("shit")
 
class DBAccess:
    "This is a wrapper around the JDBC driver to handle random DB2 failures."
 
    def __init__(self):
        Class.forName(DRIVERCLASS)
        self._conn = DriverManager.getConnection(JDBCURL, USER, PASSWORD)
 
    def create_statement(self):
        return StatementWrapper(self)
 
    def reconnect(self):
        self._conn.close()
        self._conn = DriverManager.getConnection(JDBCURL, USER, PASSWORD)
 
class StatementWrapper:
 
    def __init__(self, dba):
        self._dba = dba
        self._stmt = self._create()
 
    def _create(self):
        return dba._conn.createStatement()
 
    def execute(self, query, attempt = 1):
        try:
            return self._stmt.execute(query)
        except SqlException, e:
            self.reconnect(e)
            print "Retrying..."
            if attempt < 11:
                self.execute(query, attempt + 1)
                print "Successful"
 
    def reconnect(self, e):
            print "ERROR:", e
            # 2055, 11259: client re-route does not occur
            # -913, 57033: deadlock or timeout
            # -401, 42818: the operands of an operator or function are not compatible
            # 2030, 11211: a communication error has been detected
            print e.getMessage()
            print e.getSqlca()
            print "SQL state:", e.getSQLState()
            print "Error code:", e.getErrorCode()
            sqlstate = int(e.getSQLState())
            if sqlstate > 42000 and sqlstate < 43000:
                raise e # these are permanent errors
            self._stmt.close()
            self._dba.reconnect()
            self._stmt = self._create()
            print "Reconnected"
 
    def getResultSet(self):
        return self._stmt.getResultSet()
 
    def close(self):
        self._stmt.close()
 
# http://www.channeldb2.com/profiles/blogs/porting-limit-and-offset
 
class ResultSetMap:
 
    def __init__(self, rs):
        self._rs = rs
 
    def __getitem__(self, key):
        str = self._rs.getString(key)
        str = str.strip()
        return str or "empty"
 
# --- UTILITIES
 
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
 
def get_ns(prop):
    ix = prop.rfind('/')
    return prop[ : ix + 1]
 
from java.lang import Class
from java.sql import DriverManager
 
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
 
# --- CONFIG
 
DRIVERCLASS = "com.ibm.db2.jcc.DB2Driver"
JDBCURL = "jdbc:db2://server:5023/thingy"
USER = "LGB2990"
PASSWORD = "secret"
 
dba = DBAccess()
server = Server("TPS SDshare server", "Lars Marius Garshol")
 
coll = Collection("TPS person data", "tps-person",
                  "http://psi.nav.no/2012/mod/tps-person-graph")
server.add_collection(coll)
 
feed = SQLFragmentFeed("http://psi.garshol.priv.no/tmp/%s",
                       "person_id_tps", "tidspkt_reg", "tp411t.t_personnavn",
                       "http://psi.garshol.priv.no/tmp/Person",
                       DB2SQLBuilder(),
                       filter = "type_endring = 'S'")
feed.add_column(Column("fornavn", "http://psi.nav.no/2012/mod/nav/fornavn"))
feed.add_column(Column("mellomnavn", "http://psi.nav.no/2012/mod/nav/mellomnavn"))
feed.add_column(Column("etternavn", "http://psi.nav.no/2012/mod/nav/etternavn"))
feed.add_column(Column("pikenavn", "http://psi.nav.no/2012/mod/nav/pikenavn"))
coll.add_feed(feed)
 
 
coll = Collection("TPS: person/adresse", "tps-person-2",
                  "http://psi.nav.no/2012/mod/tps-person-2-graph")
server.add_collection(coll)
 
feed = SQLFragmentFeed("http://psi.nav.no/2012/mod/tps/adresse/%s",
                       "adresse_id", "tidspkt_reg", "tp411t.t_adresse_bosted",
                       "http://psi.garshol.priv.no/tmp/Adresse",
                                           DB2SQLBuilder(),
                       filter = "type_endring = 'S'")
feed.add_column(Column("person_id_tps", "http://psi.nav.no/2012/mod/nav/adresse-eier",
                                           False, "http://psi.garshol.priv.no/tmp/%s"))
coll.add_feed(feed)
 
 
coll = Collection("TPS-personnummer", "tps-person-3",
                  "http://psi.nav.no/2012/mod/tps-person-3-graph")
server.add_collection(coll)
 
feed = SQLFragmentFeed("http://psi.garshol.priv.no/tmp/%s",
                       "person_id_tps", "dato_endret", "tp411t.t_person",
                       "http://psi.garshol.priv.no/tmp/Person",
                                           DB2SQLBuilder())
feed.add_column(Column("person_id_off", "http://psi.nav.no/2012/mod/nav/fodselsnr"))
coll.add_feed(feed)
 
 
coll = Collection("TPS adresser", "tps-adresser",
                  "http://psi.nav.no/2012/mod/tps-adresser")
server.add_collection(coll)
 
feed = SQLFragmentFeed("http://psi.nav.no/2012/mod/tps/adresse/%s",
                       "adresse_id", "DOESNTEXIST", "tp411t.t_adresse_off",
                       "http://psi.garshol.priv.no/tmp/Adresse",
                                           DB2SQLBuilder())
feed.add_column(Column("postnr", "http://psi.nav.no/2012/mod/nav/poststed",
                                           False, "http://psi.nav.no/2012/mod/tps/postnr/%s"))
feed.add_column(Column("husnr", "http://psi.nav.no/2012/mod/nav/husnr"))
feed.add_column(Column("kommunenr", "http://psi.nav.no/2012/mod/nav/kommunenr"))
feed.add_column(Column("bokstav", "http://psi.nav.no/2012/mod/nav/bokstav"))
feed.add_column(Column("postnr", "http://psi.nav.no/2012/mod/nav/sted"))
feed.add_column(FormatColumn("http://psi.nav.no/2012/mod/nav/i-gate","http://psi.nav.no/2012/mod/tps/gateadr/%(kommunenr)s/%(gatekode)s/%(lopenr)s"))
coll.add_feed(feed)
 
 
coll = Collection("TPS epost", "tps-person-5",
                  "http://psi.nav.no/2012/mod/tps-person-5-graph")
server.add_collection(coll)
 
feed = SQLFragmentFeed("http://psi.garshol.priv.no/tmp/%s",
                       "person_id_tps", "DATO_ENDRET", "tp411t.t_epostadresse",
                       "http://psi.garshol.priv.no/tmp/Person",
                                           DB2SQLBuilder())
feed.add_column(Column("epostadrnavn", "http://psi.nav.no/2012/mod/nav/epost"))
coll.add_feed(feed)
 
 
 
coll = Collection("TPS postnummer", "tps-postnr",
                  "http://psi.nav.no/2012/mod/tps-postnr")
server.add_collection(coll)
 
feed = SQLFragmentFeed("http://psi.nav.no/2012/mod/tps/postnr/%s",
                       "postnr", "DATO_ENDRET", "tp411t.t_postreg",
                       "http://psi.garshol.priv.no/tmp/Postnummer",
                                           DB2SQLBuilder(),
                       filter = "dato_gyldig_tom = '9999-12-31'")
feed.add_column(Column("postnr", "http://psi.nav.no/2012/mod/nav/postnr"))
feed.add_column(Column("beskr_kort", "http://psi.nav.no/2012/mod/nav/beskr_kort"))
coll.add_feed(feed)
 
 
coll = Collection("TPS gateadresser", "tps-gateadresse",
                  "http://psi.nav.no/2012/mod/tps-gateadresse")
server.add_collection(coll)
 
feed = SQLFragmentFeed("http://psi.nav.no/2012/mod/tps/gateadr/%(kommunenr)s/%(gatekode)s/%(lopenr)s",
                               ("kommunenr", "gatekode", "lopenr"),
                               "DOESNT_EXIST", "tp411t.t_gateadresse",
                               "http://psi.garshol.priv.no/tmp/Gateadresse",
                               DB2SQLBuilder())
feed.add_column(Column("gatenavn", "http://psi.nav.no/2012/mod/nav/gatenavn"))
coll.add_feed(feed)
 
# --- INIT
 
web.config.debug = True
web.webapi.internalerror = web.debugerror
 
appdir = os.path.dirname(__file__)
render = web.template.render(os.path.join(appdir, 'templates/'))
app = web.application(urls, globals(), autoreload = False)
 
if __name__ == "__main__":
    app.run()
