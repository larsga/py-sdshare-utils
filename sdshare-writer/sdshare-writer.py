# implements a simple "POST rdf here" service.  this means we need a
# separate backend in the SDshare client.
# http://www.w3.org/TR/sparql11-http-rdf-update/

import sys, anydbm
import web

from java.io import FileReader
from no.priv.garshol.duke.utils import NTriplesParser

urls = (
    '/', 'HandleData',
    )

# clear:
#   wipe id-to-key store
#   truncate table

class HandleData:

    def POST(self):
        subject = web.input().get("resource")
        data = web.input().get("data")
        print repr(data)
        object = parse_data(data)
        print object
        sql = generate_sql(object)
        write_to_db(sql)
        conn.commit()
        pkeyman.commit()

        web.header("Content-Type","text/plain")
        return "OK"

# ---------------------------------------------------------------------------

ID_FIELD = "__ID__"
TYPE_FIELD = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
PKEY_FIELD = "http://psi.nav.no/2012/mod/meta/pkey-field"

def generate_sql(object):
    id = object[ID_FIELD]
    pkey = pkeyman.find_pkey_for(id)
    if not pkey:
        return generate_insert(object)
    else:
        return generate_update(object, pkey)

def generate_insert(object):
    pkey = pkeyman.generate_pkey(object)
    table = find_table_name(object)

    values = make_field_values(object, pkey)

    return "insert into %s (%s) values (%s)" % \
        (table,
         commalist([field for (field, value) in values]),
         commalist([sql(value) for (field, value) in values]))

def generate_update(object, pkey):
    table = find_table_name(object)
    values = make_field_values(object, pkey, False)

    pkey_field = find_pkey_field(object)
    return ("update %s set %s where %s = %s" %
            (table,
             commalist(["%s = %s" % (to_name(f), sql(v)) for (f, v) in values]),
             pkey_field, sql(pkey)))

def find_table_name(object):
    return to_name(object[TYPE_FIELD])

def to_name(uri):
    pos = uri.rfind('/')
    return uri[pos + 1 : ]

#def find_pkey_field(object):
#    return object[PKEY_FIELD]
def find_pkey_field(object):
    type = object[TYPE_FIELD]
    return pkey_fields[type]

# this makes a first, literal translation of the RDF, ignoring the
# configuration

def literal_translation(object, id = None, include_id = True):
    values = [(to_name(f), v) for (f, v) in object.items()
              if f not in (TYPE_FIELD, ID_FIELD, PKEY_FIELD)]

    if include_id:
        pkey_field = find_pkey_field(object)
        values.append((pkey_field, id))

    newobject = {}
    for (f, v) in values:
        newobject[f] = v
    return newobject

# takes the literally translated object and applies the configuration to
# produce the final version

def make_field_values(object, id = None, include_id = True):
    object = literal_translation(object, id, include_id)
    print "newobject:", object
    for (f, pattern) in compound_fields:
        value = pattern % ObjectWrapper(object)
        if value.strip():
            object[f] = value.strip()

    for f in skip_fields:
        try:
            del object[f]
        except KeyError:
            pass # if we don't have the field, no problem

    return object.items()

def commalist(list):
    return ", ".join(list)

def sql(value):
    if type(value) == int:
        return str(value)
    else:
        return "'" + value + "'"

def remap(v, fromv, tov):
    if v == fromv:
        return tov
    else:
        return v
        
class ObjectWrapper:

   def __init__(self, object):
        self._object = object
        
   def __getitem__(self, key):
        return self._object.get(key, "").strip()

# ---------------------------------------------------------------------------

class PrimaryKeyManager:

    def __init__(self):
        self._id_to_pkey = {} #anydbm.open('id-to-pkey.dbm', 'c')
        self._previd = 0

    def find_pkey_for(self, id):
        return self._id_to_pkey.get(id)

    def generate_pkey(self, object):
        id = object[ID_FIELD]
        self._previd += 1
        pkey = self._previd
        self._id_to_pkey[str(id)] = str(pkey)
        return pkey

    def commit(self):
        self._id_to_pkey.sync()
    
    def close(self):
        pass #self._id_to_pkey.close()

# ---------------------------------------------------------------------------

from no.priv.garshol.duke import StatementHandler

class DictMapper(StatementHandler):

    def __init__(self):
        self._dict = {}

    def statement(self, subj, prop, obj, lit):
        self._dict[ID_FIELD] = subj
        self._dict[prop] = obj

    def get_dict(self):
        return self._dict

def parse_data(rdf_string):
    from java.io import StringReader

    mapper = DictMapper()
    NTriplesParser.parse(StringReader(rdf_string), mapper)
    return mapper.get_dict()

# ---------------------------------------------------------------------------

from java.lang import Class
from java.sql import DriverManager

DRIVERCLASS = "oracle.jdbc.driver.OracleDriver"
JDBCURL = "jdbc:oracle:thin:@d26dbbl002.test.local:1521:mod01"
USER = "mod"
PASSWORD = "xlesDGn3"

Class.forName(DRIVERCLASS)
conn = DriverManager.getConnection(JDBCURL, USER, PASSWORD)
stmt = conn.createStatement()

def write_to_db(sql):
    stmt.execute(sql)

# def write_to_db(sql):
#     print sql

# conn.commit()
# stmt.close()
# conn.close()

# ---------------------------------------------------------------------------

pkeyman = PrimaryKeyManager()

# object = {
#     ID_FIELD : "http://ex/adr/342343",
#     TYPE_FIELD : "http://ex/Adressebruk",
#     PKEY_FIELD : "adressebruk_id",
#     "http://ex/adressekode" : "STILL",
#     "http://ex/landkode" : "NO",
#     "http://ex/postnr" : "0440",
#     }

# sql = generate_sql(object)
# print sql
# write_to_db(sql)

# mapper = DictMapper()
# fr = FileReader(sys.argv[1])
# NTriplesParser.parse(fr, mapper)
# fr.close()

# sql = generate_sql(mapper.get_dict())
# print sql
# #write_to_db(sql)

# ---------------------------------------------------------------------------
# had to introduce some configuration, unfortunately.

skip_fields = set(["husnr", "gatenavn", "bokstav"])
compound_fields = [("adrlinje1", "%(gatenavn)s %(husnr)s%(bokstav)s")]
pkey_fields = {"http://psi.garshol.priv.no/tmp/Adresse" : "adressebruk_id",
               "http://psi.garshol.priv.no/tmp/Person" : "person_id",
               "http://example.com/Person" : "person_id",
               "http://example.com/Adressebruk" : "adressebruk_id"}

# ---------------------------------------------------------------------------

#web.config.debug = False
web.webapi.internalerror = web.debugerror
app = web.application(urls, globals())
#app.internalerror = Error

if __name__ == "__main__":
    app.run()

pkeyman.close()
