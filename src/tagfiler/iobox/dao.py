'''
Created on Sep 10, 2012

@author: smithd
'''
import sqlite3
import logging
import os
import time
from tagfiler import iobox
from tagfiler.iobox.models import *

from logging import INFO
logger = logging.getLogger(__name__)

class DataDAO(object):
    sql_source_dir = os.path.join(os.path.dirname(iobox.__file__), "sql/")
   
    def __init__(self, db_filepath, **kwargs):
        """Constructs a DAO instance, creating the database schema in the database file if necessary
        
        Keyword arguments:
        db_filepath -- filepath to use for the sqlite database
        
        """
        def _dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
    
        self.db_filepath = db_filepath
        # create outbox file if it doesn't exist
        if not os.path.exists(self.db_filepath) and logger.isEnabledFor(INFO):
            logger.info("Database file %s doesn't exist, creating." % self.db_filepath)
        self.db = sqlite3.connect(self.db_filepath, detect_types=sqlite3.PARSE_DECLTYPES)
        self.db.row_factory = _dict_factory
    
    def close(self):
        if self.db is not None:
            self.db.close()

    def _create_db_from_source(self, db, source_file):
            cursor = db.cursor()
            f = open(source_file, "r")
            sql_stmts = str.split(f.read(), ";")
            for s in sql_stmts:
                if logger.isEnabledFor(INFO):
                    logger.info("Executing statement %s" % s)
                cursor.execute(s)
            f.close()
            cursor.close()

class OutboxDAO(DataDAO):
    """Data access object for the global outbox catalog.
    
    """
    def __init__(self, db_filepath, **kwargs):
        """Initializes an outbox data access instance.
        
        Keyword arguments:
        db_filepath -- filepath of the sqlite database
        """
        super(OutboxDAO, self).__init__(db_filepath, **kwargs)
        
        # create outbox schema if it doesn't exist
        self._create_db_from_source(self.db, os.path.join(self.__class__.sql_source_dir, "outbox.sql"))
        
    def get_state_dao(self, outbox):
        """Returns the DAO for an outbox's state.
        
        Keyword arguments:
        outbox -- outbox catalog object
        """
        
        outbox_state_filepath = os.path.join(os.path.dirname(self.db_filepath), "outbox_%i.db" % outbox.get_id())
        return OutboxStateDAO(outbox, outbox_state_filepath)

    def find_outbox_by_name(self, outbox_name):
        """Retrieves an outbox catalog object by its assigned name, or None if it doesn't exist.
        
        Keyword arguments:
        outbox_name -- name of the outbox
        
        """
        outbox = None
        cursor = self.db.cursor()
        p = (outbox_name,)
        cursor.execute("SELECT o.id AS outbox_id, o.name as outbox_name, o.tagfiler_id, t.username AS tagfiler_username, t.password AS tagfiler_password FROM outbox o INNER JOIN tagfiler AS t ON (o.tagfiler_id=t.id) WHERE o.name=?", p)
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            outbox = Outbox(**r)
            outbox.set_roots(self.find_outbox_roots(outbox))
            outbox.set_inclusion_patterns(self.find_outbox_inclusion_patterns(outbox))
            outbox.set_exclusion_patterns(self.find_outbox_exclusion_patterns(outbox))
            outbox.set_path_matches(self.find_outbox_path_matches(outbox))
            outbox.set_line_matches(self.find_outbox_line_matches(outbox))
        return outbox

    def find_outbox_roots(self, outbox):
        """Retrieves the root search directories assigned to this outbox.
        
        Keyword arguments:
        outbox -- outbox catalog object
        
        """
        roots = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, filepath, outbox_id FROM root WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            roots.append(Root(**r))
        cursor.close()
        return roots

    def find_outbox_exclusion_patterns(self, outbox):
        """Retrieves the exclusion patterns assigned to this outbox.
        
        Keyword arguments:
        outbox -- outbox catalog object
        
        """
        exclusion = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, outbox_id, pattern FROM exclusion_pattern WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            exclusion.append(ExclusionPattern(**r))
        cursor.close()
        return exclusion

    def find_outbox_inclusion_patterns(self, outbox):
        """Retrieves the inclusion patterns assigned to this outbox.
        
        Keyword arguments:
        outbox -- outbox catalog object
        
        """
        inclusion = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, outbox_id, pattern FROM inclusion_pattern WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            inclusion.append(InclusionPattern(**r))
        cursor.close()
        return inclusion
    
    def find_tagfiler(self, **kwargs):
        """Retrieves the tagfiler configuration object that matches the given arguments.
        
        Keyword arguments:
        tagfiler_url -- URL of the tagfiler instance
        tagfiler_username -- login username for the tagfiler account
        
        """
        tagfiler = None
        cursor = self.db.cursor()
        p = (kwargs.get('tagfiler_url'), kwargs.get('tagfiler_username'))
        cursor.execute("SELECT id AS tagfiler_id, url AS tagfiler_url, username AS tagfiler_url, password AS tagfiler_password FROM tagfiler WHERE url=? AND username=?", p)
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            tagfiler = Tagfiler(r)
        return tagfiler
    
    def add_tagfiler(self, tagfiler):
        """Adds a new tagfiler configuration to the database.
        
        Keyword arguments:
        
        tagfiler -- tagfiler configuration object
        
        """
        cursor = self.db.cursor()
        p = (tagfiler.get_url(), tagfiler.get_username(), tagfiler.get_password())
        cursor.execute("INSERT INTO tagfiler (url, username, password) VALUES (?, ?, ?)", p)
        cursor.execute("SELECT last_insert_rowid() AS id")
        tagfiler.set_id(cursor.fetchone()["id"])
        cursor.close()

    def add_outbox(self, outbox):
        """Adds a new outbox configuration to the database.
        
        Keyword arguments:
        outbox -- outbox configuration object
        
        """
        cursor = self.db.cursor()
        
        # ensure tagfiler exists in the DB first
        if outbox.get_tagfiler().get_id() is None:
            t = {'tagfiler_url':outbox.get_tagfiler().get_url(), 'tagfiler_username':outbox.get_tagfiler().get_username()}
            tagfiler = self.find_tagfiler(**t)
            if tagfiler is None:
                self.add_tagfiler(outbox.get_tagfiler())
            else:
                outbox.set_tagfiler(tagfiler)

        p = (outbox.get_name(), outbox.get_tagfiler().get_id())
        cursor.execute("INSERT INTO outbox (name, tagfiler_id) VALUES (?, ?)", p)
        cursor.execute("SELECT last_insert_rowid() AS id")
        outbox.set_id(cursor.fetchone()["id"])
        cursor.close()
        return outbox

    def add_root_to_outbox(self, outbox, root):
        """Adds a new root to the outbox in the database and appends it to the local object.
        
        Keyword arguments:
        outbox -- outbox configuration object
        root -- root configuration object
        
        """
        cursor = self.db.cursor()
        p = (outbox.get_id(), root.get_filepath())
        cursor.execute("SELECT id FROM root WHERE outbox_id=? AND filepath=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO root (outbox_id, filepath) VALUES (?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() as id")
            root.set_id(cursor.fetchone()["id"])
        else:
            root.set_id(r["id"])
        outbox.add_root(root)
        cursor.close()

    def add_inclusion_pattern_to_outbox(self, outbox, inclusion_pattern):
        """Adds a new inclusion pattern to the outbox in the database and appends it to the local object.
        
        Keyword arguments:
        outbox -- outbox configuration object
        inclusion_pattern -- inclusion pattern object
        
        """
        cursor = self.db.cursor()
        p = (outbox.get_id(), inclusion_pattern.get_pattern())
        cursor.execute("SELECT id FROM inclusion_pattern WHERE outbox_id=? AND pattern=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO inclusion_pattern (outbox_id, pattern) VALUES (?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            inclusion_pattern.set_id(cursor.fetchone()["id"])
        else:
            inclusion_pattern.set_id(r["id"])
        outbox.add_inclusion_pattern(inclusion_pattern)
        
    def add_exclusion_pattern_to_outbox(self, outbox, exclusion_pattern):
        """Adds a new exclusion pattern to the outbox in the database and appends it to the local object.
        
        Keyword arguments:
        outbox -- outbox configuration object
        exclusion_pattern -- exclusion pattern object
        
        """
        cursor = self.db.cursor()
        p = (outbox.get_id(), exclusion_pattern.get_pattern())
        cursor.execute("SELECT id FROM exclusion_pattern WHERE outbox_id=? AND pattern=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO exclusion_pattern (outbox_id, pattern) VALUES (?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            exclusion_pattern.set_id(cursor.fetchone()["id"])
        else:
            exclusion_pattern.set_id(r["id"])
        outbox.add_exclusion_pattern(exclusion_pattern)
    
    def add_path_match_to_outbox(self, outbox, path_match):
        """Adds a path match rule to the outbox in the database and appends it to the local object.
        
        Keyword arguments:
        outbox -- outbox configuration object
        path_match -- path match rule object
        
        """
        cursor = self.db.cursor()
        p = (outbox.get_id(), path_match.get_name(), path_match.get_pattern(), path_match.get_extract())
        cursor.execute("SELECT id FROM path_match WHERE outbox_id=? AND name=? AND pattern=? AND extract=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO path_match (outbox_id, name, pattern, extract) VALUES (?, ?, ?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            path_match.set_outbox_id(outbox.get_id())
            path_match.set_id(cursor.fetchone()['id'])
        else:
            path_match.set_id(r['id'])
        cursor.close()

        for tag in path_match.get_tags():
            self.add_path_match_tag(path_match, tag)
            
        for template in path_match.get_templates():
            self.add_path_match_template(path_match, template)

        outbox.add_path_match(path_match)
    
    def add_line_match_to_outbox(self, outbox, line_match):
        """Adds a line match rule to the outbox in the database and appends it to the local object.
        
        Keyword arguments:
        outbox -- outbox configuration object
        line_match -- line match rule object
        
        """
        cursor = self.db.cursor()
        p = (outbox.get_id(), line_match.get_name(), line_match.get_path_rule().get_pattern())
        cursor.execute("SELECT l.id FROM line_match AS l INNER JOIN path_rule AS p ON (l.path_rule_id=p.id) WHERE l.outbox_id=? AND l.name=? AND p.pattern=?", p)
        r = cursor.fetchone()
        if r is None:
            if line_match.get_path_rule().get_id() is None:
                self.add_path_rule(line_match.get_path_rule())
            p = (outbox.get_id(), line_match.get_name(), line_match.get_path_rule().get_id())
            cursor.execute("INSERT INTO line_match (outbox_id, name, path_rule_id) VALUES (?, ?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            line_match.set_outbox_id(outbox.get_id())
            line_match.set_id(cursor.fetchone()["id"])
            for line_rule in line_match.get_line_rules():
                self.add_line_match_rule(line_match, line_rule)
        else:
            line_match.set_id(r["id"])
        cursor.close()
        outbox.add_line_match(line_match)
        
    def add_line_match_rule(self, line_match, line_rule):
        """Adds a line rule to the line match rule in the database.
        
        Keyword arguments:
        line_match -- line match object
        line_rule -- line rule object
        
        """
        cursor = self.db.cursor()
        if line_rule.get_prepattern() is not None:
            self.add_line_rule_prepattern(line_rule.get_prepattern())

        p = (line_match.get_id(), line_rule.get_pattern(), line_rule.get_apply(), line_rule.get_extract(), line_rule.get_prepattern().get_id())
        cursor.execute("SELECT id FROM line_rule WHERE line_match_id=? AND pattern=? AND apply=? AND extract=? AND line_rule_prepattern_id=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO line_rule (line_match_id, pattern, apply, extract, line_rule_prepattern_id) VALUES (?, ?, ?, ?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            line_rule.set_line_match_id(line_match.get_id())
            line_rule.set_id(cursor.fetchone()['id'])
        else:
            line_rule.set_id(r['id'])
        cursor.close()
    
    def add_line_rule_prepattern(self, line_rule_prepattern):
        """Adds a line rule prepattern to the database.
        
        Keyword arguments:
        line_rule_prepattern: line rule prepattern object
        
        """
        cursor = self.db.cursor()
        p = (line_rule_prepattern.get_pattern(),)
        cursor.execute("SELECT id FROM line_rule_prepattern WHERE pattern=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO line_rule_prepattern (pattern) VALUES (?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            line_rule_prepattern.set_id(cursor.fetchone()["id"])
        else:
            line_rule_prepattern.set_id(r['id'])
        cursor.close()

    def add_path_rule(self, path_rule):
        """Adds a path rule to the database.
        
        Keyword arguments:
        path_rule -- path rule object
        
        """
        cursor = self.db.cursor()
        p = (path_rule.get_pattern(),)
        cursor.execute("SELECT id FROM path_rule WHERE pattern = ?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO path_rule (pattern) VALUES (?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            path_rule.set_id(cursor.fetchone()["id"])
        else:
            path_rule.set_id(r["id"])
        cursor.close()

    def add_path_match_tag(self, path_match, tag):
        """Adds a tag to a path_match rule in the database.
        
        Keyword arguments:
        path_match -- path_match object
        tag -- tag object
        
        """
        cursor = self.db.cursor()
        p = (path_match.get_id(), tag.get_tag_name())
        cursor.execute("SELECT id FROM path_match_tag WHERE path_match_id=? AND tag_name=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO path_match_tag (path_match_id, tag_name) VALUES (?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            tag.set_path_match_id(path_match.get_id())
            tag.set_id(cursor.fetchone()["id"])
        else:
            tag.set_id(r['id'])
        cursor.close()

    def add_path_match_template(self, path_match, template):
        """Adds a path match template to the database.
        
        Keyword arguments:
        path_match: path match object
        template: path match template object
        
        """
        cursor = self.db.cursor()
        p = (path_match.get_id(), template.get_template())
        cursor.execute("SELECT id FROM path_match_template WHERE path_match_id=? AND template=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO path_match_template (path_match_id, template) VALUES (?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            template.set_path_match_id(path_match.get_id())
            template.set_id(cursor.fetchone()["id"])
        else:
            template.set_id(cursor.fetchone()["id"])
        cursor.close()

    def find_outbox_path_matches(self, outbox):
        """Returns all of the path match rules assigned to the outbox.
        
        Keyword arguments:
        outbox -- outbox configuration object
        
        """
        path_matches = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, outbox_id, name, pattern, extract FROM path_match WHERE outbox_id=?", p)
        result = cursor.fetchall()
        cursor.close()
        for r in result:
            pm = PathMatch(**r)
            pm.set_tags(self.find_path_match_tags(pm))
            pm.set_templates(self.find_path_match_templates(pm))
            path_matches.append(pm)
        return path_matches

    def find_outbox_line_matches(self, outbox):
        """Returns all of the line match rules assigned to the outbox.
        
        Keyword arguments:
        outbox -- outbox configuration object
        
        """
        line_matches = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT l.id, l.outbox_id, l.name, l.path_rule_id, p.pattern FROM line_match AS l INNER JOIN path_rule AS p ON (l.path_rule_id=p.id) WHERE l.outbox_id=?", p)
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            line_match = LineMatch(**r)
            line_match.set_line_rules(self.find_line_match_line_rules(line_match))
            line_matches.append(line_match)
        return line_matches
    
    def find_line_match_line_rules(self, line_match):
        """Returns all of the line rules assigned to a line match rule.
        
        Keyword arguments:
        line_match -- line match rule object
        
        """
        line_rules = []
        cursor = self.db.cursor()
        p = (line_match.get_id(),)
        cursor.execute("SELECT l.id, l.line_rule_prepattern_id, l.pattern, l.apply, l.extract, l.line_match_id, p.pattern as line_rule_prepattern FROM line_rule AS l LEFT JOIN line_rule_prepattern AS p ON (l.line_rule_prepattern_id=p.id) WHERE l.line_match_id=?", p)
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            line_rules.append(LineRule(**r))
        return line_rules

    def find_path_match_tags(self, path_match):
        """Returns all of the tags assigned to a path match rule.
        
        Keyword arguments:
        path_match -- path match rule object
        
        """
        tags = []
        cursor = self.db.cursor()
        p = (path_match.get_id(),)
        cursor.execute("SELECT id, path_match_id, tag_name FROM path_match_tag WHERE path_match_id = ?", p)
        for r in cursor.fetchall():
            tags.append(PathMatchTag(**r))
        cursor.close()
        return tags

    def find_path_match_templates(self, path_match):
        """Returns all of the templates assigned to a path match rule.
        
        Keyword arguments:
        path_match -- path match rule object
        
        """
        templates = []
        cursor = self.db.cursor()
        p = (path_match.get_id(),)
        cursor.execute("SELECT id, path_match_id, template FROM path_match_template WHERE path_match_id=?", p)
        for r in cursor.fetchall():
            templates.append(PathMatchTemplate(**r))
        cursor.close()
        return templates

class OutboxStateDAO(DataDAO):
    """Data Access Object for a particular outbox's state.
    
    """
    
    def __init__(self, outbox, db_file):
        super(OutboxStateDAO, self).__init__(db_file)
        self.outbox = outbox
        
        # create the outbox instance if it doesn't exist
        self._create_db_from_source(self.db, os.path.join(self.__class__.sql_source_dir, "outbox_instance.sql"))

    def find_scan_state(self, state_name):
        """Returns the scan state object for a given scan state name.
        
        Keyword arguments:
        state_name: state name
        
        """
        state = ScanState()
        state.set_state(state_name)
        cursor = self.db.cursor()
        p = (state_name,)
        cursor.execute("SELECT id FROM scan_state WHERE state=?", p)
        r = cursor.fetchone()
        cursor.close()
        if r is None:
            cursor.execute("INSERT INTO scan_state (state) VALUES (?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            state.set_id(cursor.fetchone()['id'])
        else:
            state.set_id(r['id'])
        return state

    def find_all_scans(self):
        """Returns all scans from the database.
        
        """
        scans = []
        cursor = self.db.cursor()
        cursor.execute("SELECT s.id, s.start, s.end, s.scan_state_id, st.state FROM scan AS s INNER JOIN scan_state AS st ON (s.scan_state_id=st.id)")
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            scans.append(Scan(**r))
        return scans
    
    def start_file_scan(self):
        """Adds a new scan and returns a scan object.
        
        """
        start_state = self.find_scan_state('START_FILE_SCAN')
        cursor = self.db.cursor()
        p = (time.time(), start_state.get_id())
        cursor.execute("INSERT INTO scan (start, scan_state_id) VALUES (?, ?)", p)
        cursor.execute("SELECT last_insert_rowid() AS id")
        p = (cursor.fetchone()['id'],)
        cursor.execute("SELECT s.id, s.start, s.end, s.scan_state_id, st.state FROM scan AS s INNER JOIN scan_state AS st ON (s.scan_state_id=st.id) WHERE s.id=?", p)
        r = cursor.fetchone()
        cursor.close()
        return Scan(**r)

    def find_last_scan(self):
        """Retrieves the last scan that was started.
        
        """
        scan = None
        cursor = self.db.cursor()
        cursor.execute("SELECT s.id, s.start, s.end, s.scan_state_id, st.state FROM scan AS s INNER JOIN scan_state AS st ON (s.scan_state_id=st.id) ORDER BY s.end DESC LIMIT 1")
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            scan = Scan(**r)
            scan.set_files(self.find_files_in_scan(scan))
        return scan
    
    def find_file_by_path(self, filepath):
        """Retrieves a file object from the database matching the file path.
        
        Keyword arguments:
        filepath -- fully qualified filename
        
        """
        f = None
        cursor = self.db.cursor()
        p = (filepath,)
        cursor.execute("SELECT id, filepath, mtime, size, checksum, must_tag FROM file WHERE filepath=?", p)
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            f = File(**r)
        return f
    
    def add_file(self, f):
        """Adds a new file object to the database.
        
        Keyword arguments:
        f -- file object
        
        """
        cursor = self.db.cursor()
        p = (f.get_filepath(),)
        cursor.execute("SELECT id FROM file WHERE filepath=?", p)
        r = cursor.fetchone()
        if r is None:
            p = (f.get_filepath(), f.get_mtime(), f.get_size(), f.get_checksum(), f.get_must_tag())
            cursor.execute("INSERT INTO file (filepath, mtime, size, checksum, must_tag) VALUES (?, ?, ?, ?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            f.set_id(cursor.fetchone()["id"])
        else:
            f.set_id(r["id"])
        cursor.close()
        return f

    def update_file(self, f):
        """Saves information from an existing file object to the database.
        
        Keyword arguments:
        f -- file object
        
        """
        cursor = self.db.cursor()
        p = (f.get_filepath(), f.get_mtime(), f.get_size(), f.get_checksum(), f.get_must_tag(), f.get_id())
        cursor.execute("UPDATE file SET filepath=?, mtime=?, size=?, checksum=?, must_tag=? WHERE id=?", p)
        cursor.close()

    def add_file_to_scan(self, scan, f):
        """Adds an existing file to an existing scan in the database.
        
        Keyword arguments:
        scan -- scan object
        file -- file object
        
        """
        if f.get_id() is None:
            self.add_file(f)
        cursor = self.db.cursor()
        p = (scan.get_id(), f.get_id())
        cursor.execute("SELECT 1 FROM scan_files WHERE scan_id=? AND file_id=?", p)
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO scan_files (scan_id, file_id) VALUES (?, ?)", p)
        cursor.close()

    def find_files_in_scan(self, scan):
        """Returns all files associated with a particular scan.
        
        Keyword arguments:
        scan -- scan object
        
        """
        cursor = self.db.cursor()
        p = (scan.get_id(),)
        files = []
        cursor.execute("SELECT f.id, f.filepath, f.mtime, f.size, f.checksum, f.must_tag FROM scan_files AS s INNER JOIN file AS f ON (s.file_id=f.id) WHERE s.scan_id=?", p)
        for r in cursor.fetchall():
            files.append(File(**r))
        cursor.close()
        return files

    def finish_file_scan(self, scan):
        """Completes a scan.
        
        Keyword arguments:
        scan -- the scan to complete
        
        """
        completed_state = self.find_scan_state('COMPLETED_FILE_SCAN')
        end_time = time.time()
        cursor = self.db.cursor()
        p = (completed_state.get_id(), end_time, scan.get_id())
        cursor.execute("UPDATE scan SET scan_state_id=?, end=? WHERE id=?", p)
        cursor.close()
        scan.set_state(completed_state)
        scan.set_end(end_time)

    def find_scans_to_tag(self):
        """Retrieves a list of scans that need to be tagging.
        
        """
        scans = []
        cursor = self.db.cursor()
        completed_state = self.find_scan_state('COMPLETED_FILE_SCAN')
        start_tag_state = self.find_scan_state('START_FILE_TAGGING')
        p = (completed_state.get_id(), start_tag_state.get_id())
        cursor.execute("SELECT s.id, s.start, s.end, s.scan_state_id, st.state FROM scan AS s INNER JOIN scan_state AS st ON (s.scan_state_id=st.id) WHERE s.scan_state_id=? OR s.scan_state_id=?", p)
        results = cursor.fetchall()
        for r in results:
            scan = Scan(**r)
            scan.set_files(self.find_files_in_scan(scan))
            scans.append(scan)
        cursor.close()
        return scans
    
    def register_file(self, f):
        """Registers a file to be added to tagfiler.
        
        Keyword arguments:
        f -- file object to register
        
        """
        registered_file = RegisterFile()
        registered_file.set_file(f)
        cursor = self.db.cursor()
        p = (f.get_id(),)
        cursor.execute("SELECT id FROM register_file WHERE file_id=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO register_file (file_id, added) VALUES (?, date('now'))", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            registered_file.set_id(cursor.fetchone()["id"])
        else:
            registered_file.set_id(r["id"])
        cursor.close()
        return registered_file

    def add_tag_to_registered_file(self, register_file, tag):
        """Adds a tag to include in registering a file.
        
        Keyword arguments:
        register_file -- register file object
        tag -- tag object to include
        
        """
        cursor = self.db.cursor()
        p = (register_file.get_id(), tag.get_tag_name(), tag.get_tag_value())
        cursor.execute("SELECT id FROM register_tag WHERE register_file_id=? AND tag_name=? AND tag_value=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO register_tag (register_file_id, tag_name, tag_value) VALUES (?, ?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            tag.set_id(cursor.fetchone()["id"])
        else:
            tag.set_id(r["id"])
        cursor.close()
        register_file.add_tag(tag)
        return tag

    def find_tagged_files_to_register(self):
        """Retrieves a list of all files to register.
        
        """
        files = []
        cursor = self.db.cursor()
        cursor.execute("SELECT r.id AS register_file_id, r.added, f.id, f.filepath, f.mtime, f.size, f.checksum, f.must_tag FROM register_file AS r INNER JOIN file AS f ON (r.file_id=f.id) WHERE f.must_tag='FALSE'")
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            f = RegisterFile(**r)
            f.set_tags(self.find_tags_to_register(f))
            files.append(f)
        return files

    def find_tags_to_register(self, register_file):
        """Retrieves a list of all tags to include for a file to register
        
        Keyword arguments:
        register_file -- register file object
        
        """
        tags = []
        cursor = self.db.cursor()
        p = (register_file.get_id(),)
        cursor.execute("SELECT id, register_file_id, tag_name, tag_value FROM register_tag WHERE register_file_id=?", p)
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            tags.append(RegisterTag(**r))
        return tags

    def remove_registered_file_and_tags(self, registered_file):
        """Removes the registered file and its tags from the database.
        
        Keyword arguments:
        registered_file -- the registered file to remove
        
        """
        p = (registered_file.get_id(),)
        cursor = self.db.cursor()
        cursor.execute("DELETE FROM register_tag WHERE register_file_id=?", p)
        cursor.execute("DELETE FROM register_file WHERE id=?", p)
        registered_file.set_id(None)
