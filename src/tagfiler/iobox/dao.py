'''
Created on Sep 10, 2012

@author: smithd
'''
import sqlite3
import logging
import os
from tagfiler import iobox
from tagfiler.iobox.models import *

from logging import INFO
logger = logging.getLogger(__name__)

class DataDAO(object):
    sql_source_dir = os.path.join(os.path.dirname(iobox.__file__), "sql/")
   
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
        
    def _dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

class OutboxDAO(DataDAO):
    """
    Data access object for the outbox.
    """
    def __init__(self, outbox_file, **kwargs):
        """
        Initializes an outbox data access instance.
        """
        self.outbox_file = outbox_file
        self.outbox_name = kwargs.get("outbox_name")
        
        # create outbox file if it doesn't exist
        if not os.path.exists(self.outbox_file) and logger.isEnabledFor(INFO):
            logger.info("Outbox file %s doesn't exist, creating." % self.outbox_file)
        self.outbox_db = sqlite3.connect(self.outbox_file)
        self.outbox_db.row_factory = self._dict_factory
        
        # create outbox schema if it doesn't exist
        self._create_db_from_source(self.outbox_db, os.path.join(self.__class__.sql_source_dir, "outbox.sql"))
        
        # lookup outbox by name and create it if it doesn't exist
        outbox = self.get_outbox_by_name(self.outbox_name)
        if outbox is None:
            outbox = Outbox(**kwargs)
            self.create_outbox(outbox)

    def get_instance(self, outbox):
        outbox_instance_file = os.path.join(os.path.dirname(self.outbox_file), "outbox_%i.db" % outbox.get_id())
        return OutboxInstanceDAO(outbox, outbox_instance_file)

    def close(self):
        self.outbox_db.close()

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
            outbox.set_inclusion_patterns(self.get_outbox_inclusion_patterns(outbox))
            outbox.set_exclusion_patterns(self.get_outbox_exclusion_patterns(outbox))
            outbox.set_path_matches(self.get_outbox_path_matches(outbox))
            outbox.set_line_matches(self.get_outbox_line_matches(outbox))
        return outbox

    def get_outbox_roots(self, outbox):
        roots = []
        cursor = self.outbox_db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, filename, outbox_id FROM root WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            roots.append(Root(**r))
        cursor.close()
        return roots

    def get_outbox_exclusion_patterns(self, outbox):
        exclusion = []
        cursor = self.outbox_db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, outbox_id, pattern FROM exclusion_pattern WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            exclusion.append(ExclusionPattern(**r))
        cursor.close()
        return exclusion

    def get_outbox_inclusion_patterns(self, outbox):
        inclusion = []
        cursor = self.outbox_db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, outbox_id, pattern FROM inclusion_pattern WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            inclusion.append(InclusionPattern(**r))
        cursor.close()
        return inclusion
    
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
        tagfiler.set_id(cursor.fetchone()["id"])
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
        outbox.set_id(cursor.fetchone()["id"])
        cursor.close()
        return outbox

    def add_root_to_outbox(self, outbox, root):
        cursor = self.outbox_db.cursor()
        p = (outbox.get_id(), root.get_filename())
        cursor.execute("SELECT id FROM root WHERE outbox_id=? AND filename=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO root (outbox_id, filename) VALUES (?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() as id")
            root.set_id(cursor.fetchone()["id"])
        else:
            root.set_id(r["id"])
        outbox.add_root(root)
        cursor.close()

    def add_inclusion_pattern_to_outbox(self, outbox, inclusion_pattern):
        cursor = self.outbox_db.cursor()
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
        cursor = self.outbox_db.cursor()
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
        cursor = self.outbox_db.cursor()
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
            self.save_path_match_tag(path_match, tag)
            
        for template in path_match.get_templates():
            self.save_path_match_template(path_match, template)

        outbox.add_path_match(path_match)
    
    def add_line_match_to_outbox(self, outbox, line_match):
        cursor = self.outbox_db.cursor()
        p = (outbox.get_id(), line_match.get_name(), line_match.get_path_rule().get_pattern())
        cursor.execute("SELECT l.id FROM line_match AS l INNER JOIN path_rule AS p ON (l.path_rule_id=p.id) WHERE l.outbox_id=? AND l.name=? AND p.pattern=?", p)
        r = cursor.fetchone()
        if r is None:
            if line_match.get_path_rule().get_id() is None:
                self.save_path_rule(line_match.get_path_rule())
            p = (outbox.get_id(), line_match.get_name(), line_match.get_path_rule().get_id())
            cursor.execute("INSERT INTO line_match (outbox_id, name, path_rule_id) VALUES (?, ?, ?)", p)
            cursor.execute("SELECT last_insert_rowid() AS id")
            line_match.set_outbox_id(outbox.get_id())
            line_match.set_id(cursor.fetchone()["id"])
            for line_rule in line_match.get_line_rules():
                self.save_line_match_rule(line_match, line_rule)
        else:
            line_match.set_id(r["id"])
        cursor.close()
        outbox.add_line_match(line_match)
        
    def save_line_match_rule(self, line_match, line_rule):
        cursor = self.outbox_db.cursor()
        if line_rule.get_prepattern() is not None:
            self.save_line_rule_prepattern(line_rule.get_prepattern())

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
    
    def save_line_rule_prepattern(self, line_rule_prepattern):
        cursor = self.outbox_db.cursor()
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

    def save_path_rule(self, path_rule):
        cursor = self.outbox_db.cursor()
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

    def save_path_match_tag(self, path_match, tag):
        cursor = self.outbox_db.cursor()
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

    def save_path_match_template(self, path_match, template):
        cursor = self.outbox_db.cursor()
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

    def get_outbox_path_matches(self, outbox):
        path_matches = []
        cursor = self.outbox_db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, outbox_id, name, pattern, extract FROM path_match WHERE outbox_id=?", p)
        result = cursor.fetchall()
        cursor.close()
        for r in result:
            pm = PathMatch(**r)
            pm.set_tags(self.get_path_match_tags(pm))
            pm.set_templates(self.get_path_match_templates(pm))
            path_matches.append(pm)
        return path_matches

    def get_outbox_line_matches(self, outbox):
        line_matches = []
        cursor = self.outbox_db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT l.id, l.outbox_id, l.name, l.path_rule_id, p.pattern FROM line_match AS l INNER JOIN path_rule AS p ON (l.path_rule_id=p.id) WHERE l.outbox_id=?", p)
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            line_match = LineMatch(**r)
            line_match.set_line_rules(self.get_line_match_line_rules(line_match))
            line_matches.append(line_match)
        return line_matches
    
    def get_line_match_line_rules(self, line_match):
        line_rules = []
        cursor = self.outbox_db.cursor()
        p = (line_match.get_id(),)
        cursor.execute("SELECT l.id, l.line_rule_prepattern_id, l.pattern, l.apply, l.extract, l.line_match_id, p.pattern as line_rule_prepattern FROM line_rule AS l LEFT JOIN line_rule_prepattern AS p ON (l.line_rule_prepattern_id=p.id) WHERE l.line_match_id=?", p)
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            line_rules.append(LineRule(**r))
        return line_rules

    def get_path_match_tags(self, path_match):
        tags = []
        cursor = self.outbox_db.cursor()
        p = (path_match.get_id(),)
        cursor.execute("SELECT id, path_match_id, tag_name FROM path_match_tag WHERE path_match_id = ?", p)
        for r in cursor.fetchall():
            tags.append(PathMatchTag(**r))
        cursor.close()
        return tags

    def get_path_match_templates(self, path_match):
        templates = []
        cursor = self.outbox_db.cursor()
        p = (path_match.get_id(),)
        cursor.execute("SELECT id, path_match_id, template FROM path_match_template WHERE path_match_id=?", p)
        for r in cursor.fetchall():
            templates.append(PathMatchTemplate(**r))
        cursor.close()
        return templates

class OutboxInstanceDAO(DataDAO):
    def __init__(self, outbox, outbox_instance_file):
        self.outbox = outbox
        self.outbox_instance_file = outbox_instance_file
        self.outbox_instance_db = sqlite3.connect(self.outbox_instance_file)
        self.outbox_instance_db.row_factory = self._dict_factory
        
        # create the outbox instance if it doesn't exist
        self._create_db_from_source(self.outbox_instance_db, os.path.join(self.__class__.sql_source_dir, "outbox_instance.sql"))

    def get_scan_state(self, state_name):
        state = ScanState()
        state.set_state(state_name)
        cursor = self.outbox_instance_db.cursor()
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

    def get_all_scans(self):
        scans = []
        cursor = self.outbox_instance_db.cursor()
        cursor.execute("SELECT s.id, s.start, s.end, s.scan_state_id, st.state FROM scan AS s INNER JOIN scan_state AS st ON (s.scan_state_id=st.id)")
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            scans.append(Scan(**r))
        return scans
    
    def start_file_scan(self):
        start_state = self.get_scan_state('START_FILE_SCAN')
        cursor = self.outbox_instance_db.cursor()
        p = (start_state.get_id(),)
        cursor.execute("INSERT INTO scan (start, scan_state_id) VALUES (datetime('now'), ?)", p)
        cursor.execute("SELECT last_insert_rowid() AS id")
        p = (cursor.fetchone()['id'])
        cursor.execute("SELECT s.id, s.start, s.end, s.scan_state_id, st.state FROM scan AS s INNER JOIN scan_state AS st ON (s.scan_state_id=st.id)")
        r = cursor.fetchone()
        cursor.close()
        return Scan(**r)

    def close(self):
        self.outbox_instance_db.close()
