import sqlite3
import logging
import os
from tagfiler import iobox
from logging import INFO
logger = logging.getLogger(__name__)

class OutboxDAO:
    """
    Data access object for the outbox.
    """
    sql_source_dir = os.path.join(os.path.dirname(iobox.__file__), "sql/")
    def __init__(self, outbox_file, **kwargs):
        """
        Initializes an outbox data access instance.
        """
        
        def _create_db_from_source(db, source_file):
            cursor = db.cursor()
            f = open(source_file, "r")
            sql_stmts = str.split(f.read(), ";")
            for s in sql_stmts:
                if logger.isEnabledFor(INFO):
                    logger.info("Executing statement %s" % s)
                cursor.execute(s)
            f.close()
            cursor.close()
        
        self.outbox_file = outbox_file
        self.outbox_name = kwargs.get("outbox_name")
        
        # create outbox file if it doesn't exist
        if not os.path.exists(self.outbox_file) and logger.isEnabledFor(INFO):
            logger.info("Outbox file %s doesn't exist, creating." % self.outbox_file)
        self.outbox_db = sqlite3.connect(self.outbox_file)
        
        # create outbox schema if it doesn't exist
        _create_db_from_source(self.outbox_db, os.path.join(self.__class__.sql_source_dir, "outbox.sql"))
        
        # lookup outbox by name and create it if it doesn't exist
        outbox = self.get_outbox_by_name(self.outbox_name)
        if outbox is None:
            outbox = Outbox(**kwargs)
            self.create_outbox(outbox)

        self.outbox_instance_file = os.path.join(os.path.dirname(self.outbox_file), "outbox_%i.db" % outbox.get_id())
        self.outbox_instance_db = sqlite3.connect(self.outbox_instance_file)
        
        # create the outbox instance if it doesn't exist
        _create_db_from_source(self.outbox_instance_db, os.path.join(self.__class__.sql_source_dir, "outbox_instance.sql"))
        
    def close(self):
        self.outbox_db.close()
        self.outbox_instance_db.close()

    def get_outbox_by_name(self, outbox_name):
        outbox = None
        cursor = self.outbox_db.cursor()
        p = (outbox_name,)
        cursor.execute("SELECT o.id AS outbox_id, o.name as outbox_name, o.tagfiler_id, t.username AS tagfiler_username, t.password AS tagfiler_password FROM outbox o INNER JOIN tagfiler AS t ON (o.tagfiler_id=t.id) WHERE o.name=?", p)
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            outbox = Outbox(**r)
            outbox.set_roots(self.get_outbox_roots(outbox))
        return outbox

    def get_outbox_roots(self, outbox):
        roots = []
        cursor = self.outbox_db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, filename, outbox_id FROM root WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            roots.append(Root(r))
        return roots

    def get_tagfiler(self, **kwargs):
        tagfiler = None
        cursor = self.outbox_db.cursor()
        p = (kwargs.get('tagfiler_url'), kwargs.get('tagfiler_username'))
        cursor.execute("SELECT id AS tagfiler_id, url AS tagfiler_url, username AS tagfiler_url, password AS tagfiler_password FROM tagfiler WHERE url=? AND username=?", p)
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            tagfiler = Tagfiler(r)
        return tagfiler
    
    def create_tagfiler(self, tagfiler):
        cursor = self.outbox_db.cursor()
        p = (tagfiler.get_url(), tagfiler.get_username(), tagfiler.get_password())
        cursor.execute("INSERT INTO tagfiler (url, username, password) VALUES (?, ?, ?)", p)
        cursor.execute("SELECT last_insert_rowid() AS id")
        tagfiler.set_id(cursor.fetchone()[0])
        cursor.close()

    def create_outbox(self, outbox):
        cursor = self.outbox_db.cursor()
        
        # ensure tagfiler exists in the DB first
        if outbox.get_tagfiler().get_id() is None:
            t = {'tagfiler_url':outbox.get_tagfiler().get_url(), 'tagfiler_username':outbox.get_tagfiler().get_username()}
            tagfiler = self.get_tagfiler(**t)
            if tagfiler is None:
                self.create_tagfiler(outbox.get_tagfiler())
            else:
                outbox.set_tagfiler(tagfiler)

        p = (outbox.get_name(), outbox.get_tagfiler().get_id())
        cursor.execute("INSERT INTO outbox (name, tagfiler_id) VALUES (?, ?)", p)
        cursor.execute("SELECT last_insert_rowid() AS id")
        outbox.set_id(cursor.fetchone()[0])
        cursor.close()
        return outbox

class Outbox:
    def __init__(self, **kwargs):
        self.id = kwargs.get("outbox_id")
        self.name = kwargs.get("outbox_name")
        self.tagfiler = Tagfiler(**kwargs)
        self.roots = []
        self.inclusion_patterns = []
        self.exclusion_patterns = []

    def set_id(self, i):
        self.id = i
    def get_name(self):
        return self.name
    def get_id(self):
        return self.id
    def get_tagfiler(self):
        return self.tagfiler
    def set_tagfiler(self, tagfiler):
        self.tagfiler = tagfiler
    def get_roots(self):
        return self.roots
    def set_roots(self, roots):
        self.roots = roots
    def get_inclusion_patterns(self):
        return self.inclusion_patterns
    def set_inclusion_patterns(self, inclusion_patterns):
        self.inclusion_patterns = inclusion_patterns
    def get_exclusion_patterns(self):
        return self.exclusion_patterns
    def set_exclusion_patterns(self, exclusion_patterns):
        self.exclusion_patterns = exclusion_patterns

class Tagfiler:
    def __init__(self, **kwargs):
        self.id = kwargs.get("tagfiler_id")
        self.url = kwargs.get("tagfiler_url")
        self.username = kwargs.get("tagfiler_username")
        self.password = kwargs.get("tagfiler_password")
    def get_id(self):
        return self.id
    def set_id(self, i):
        self.id = i
    def get_url(self):
        return self.url
    def get_username(self):
        return self.username
    def get_password(self):
        return self.password

class Root:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.filename = kwargs.get("filename")

# test units
import unittest

class TestOutboxDAO(unittest.TestCase):
    
    def setUp(self):
        import tempfile
        p = {'outbox_name':'test_outbox', 'tagfiler_url':'https://jacoby.isi.edu/tagfiler', 'tagfiler_username':'smithd', 'tagfiler_password':'smithd'}
        self.dao = OutboxDAO(os.path.join(tempfile.gettempdir(), "outbox.db"), **p)
    def tearDown(self):
        self.dao.close()

    def testConstructor(self):
        print "Outbox created."
    def testGetOutboxByName(self):
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox is not None
        
if __name__ == "__main__":
    unittest.main()
    