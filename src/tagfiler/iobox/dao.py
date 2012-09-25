'''
Created on Sep 10, 2012

@author: smithd
'''
import sqlite3
import logging
import os
import time
from tagfiler import iobox
import models

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
        self.db = sqlite3.connect(self.db_filepath, check_same_thread = False, detect_types=sqlite3.PARSE_DECLTYPES)
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
            self.db.commit()
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
        outbox -- models.Outbox object
        """
        assert isinstance(outbox, models.Outbox)
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
            outbox = models.Outbox(**r)
            outbox.set_roots(self.find_outbox_roots(outbox))
            outbox.set_inclusion_patterns(self.find_outbox_inclusion_patterns(outbox))
            outbox.set_exclusion_patterns(self.find_outbox_exclusion_patterns(outbox))
            outbox.set_path_rules(self.find_outbox_path_rules(outbox))
            outbox.set_line_rules(self.find_outbox_line_rules(outbox))
        return outbox

    def find_outbox_roots(self, outbox):
        """Retrieves the root search directories assigned to this outbox.
        
        Keyword arguments:
        outbox -- models.Outbox object
        
        """
        assert isinstance(outbox, models.Outbox)
        roots = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, filepath, outbox_id FROM root WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            roots.append(models.Root(**r))
        cursor.close()
        return roots

    def find_outbox_exclusion_patterns(self, outbox):
        """Retrieves the exclusion patterns assigned to this outbox.
        
        Keyword arguments:
        outbox -- models.Outbox object
        
        """
        assert isinstance(outbox, models.Outbox)
        exclusion = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, outbox_id, pattern FROM exclusion_pattern WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            exclusion.append(models.ExclusionPattern(**r))
        cursor.close()
        return exclusion

    def find_outbox_inclusion_patterns(self, outbox):
        """Retrieves the inclusion patterns assigned to this outbox.
        
        Keyword arguments:
        outbox -- models.Outbox object
        
        """
        assert isinstance(outbox, models.Outbox)
        inclusion = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT id, outbox_id, pattern FROM inclusion_pattern WHERE outbox_id=?", p)
        for r in cursor.fetchall():
            inclusion.append(models.InclusionPattern(**r))
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
            tagfiler = models.Tagfiler(r)
        return tagfiler
    
    def add_tagfiler(self, tagfiler):
        """Adds a new tagfiler configuration to the database.
        
        Keyword arguments:
        
        tagfiler -- models.Tagfiler object
        
        """
        assert isinstance(tagfiler, models.Tagfiler)
        cursor = self.db.cursor()
        p = (tagfiler.get_url(), tagfiler.get_username(), tagfiler.get_password())
        cursor.execute("INSERT INTO tagfiler (url, username, password) VALUES (?, ?, ?)", p)
        self.db.commit()
        cursor.execute("SELECT last_insert_rowid() AS id")
        tagfiler.set_id(cursor.fetchone()["id"])
        cursor.close()

    def add_outbox(self, outbox):
        """Adds a new outbox configuration to the database.
        
        Keyword arguments:
        outbox -- models.Outbox object
        
        """
        assert isinstance(outbox, models.Outbox)
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
        self.db.commit()
        cursor.execute("SELECT last_insert_rowid() AS id")
        outbox.set_id(cursor.fetchone()["id"])
        cursor.close()
        return outbox

    def add_root_to_outbox(self, outbox, root):
        """Adds a new root to the outbox in the database and appends it to the local object.
        
        Keyword arguments:
        outbox -- models.Outbox object
        root -- models.Root object
        
        """
        assert isinstance(outbox, models.Outbox)
        assert isinstance(root, models.Root)

        cursor = self.db.cursor()
        p = (outbox.get_id(), root.get_filepath())
        cursor.execute("SELECT id FROM root WHERE outbox_id=? AND filepath=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO root (outbox_id, filepath) VALUES (?, ?)", p)
            self.db.commit()
            cursor.execute("SELECT last_insert_rowid() as id")
            root.set_id(cursor.fetchone()["id"])
            self.db.commit()
        else:
            root.set_id(r["id"])
        outbox.add_root(root)
        cursor.close()

    def add_inclusion_pattern_to_outbox(self, outbox, inclusion_pattern):
        """Adds a new inclusion pattern to the outbox in the database and appends it to the local object.
        
        Keyword arguments:
        outbox -- models.Outbox object
        inclusion_pattern -- models.InclusionPattern object
        
        """
        assert isinstance(outbox, models.Outbox)
        assert isinstance(inclusion_pattern, models.InclusionPattern)

        cursor = self.db.cursor()
        p = (outbox.get_id(), inclusion_pattern.get_pattern())
        cursor.execute("SELECT id FROM inclusion_pattern WHERE outbox_id=? AND pattern=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO inclusion_pattern (outbox_id, pattern) VALUES (?, ?)", p)
            self.db.commit()
            cursor.execute("SELECT last_insert_rowid() AS id")
            inclusion_pattern.set_id(cursor.fetchone()["id"])
        else:
            inclusion_pattern.set_id(r["id"])
        outbox.add_inclusion_pattern(inclusion_pattern)
        cursor.close()

    def add_exclusion_pattern_to_outbox(self, outbox, exclusion_pattern):
        """Adds a new exclusion pattern to the outbox in the database and appends it to the local object.
        
        Keyword arguments:
        outbox -- models.Outbox object
        exclusion_pattern -- models.ExclusionPattern object
        
        """
        assert isinstance(outbox, models.Outbox)
        assert isinstance(exclusion_pattern, models.ExclusionPattern)

        cursor = self.db.cursor()
        p = (outbox.get_id(), exclusion_pattern.get_pattern())
        cursor.execute("SELECT id FROM exclusion_pattern WHERE outbox_id=? AND pattern=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO exclusion_pattern (outbox_id, pattern) VALUES (?, ?)", p)
            self.db.commit()
            cursor.execute("SELECT last_insert_rowid() AS id")
            exclusion_pattern.set_id(cursor.fetchone()["id"])
        else:
            exclusion_pattern.set_id(r["id"])
        outbox.add_exclusion_pattern(exclusion_pattern)
        cursor.close()

    def add_path_rule(self, path_rule):
        """Adds a path_rule to the database.
        
        keyword arguments:
        path_rule -- models.PathRule object
        """
        assert isinstance(path_rule, models.PathRule)

        self.add_rerule(path_rule)
        cursor = self.db.cursor()
        p = (path_rule.get_id(),)
        cursor.execute("INSERT INTO path_rule (rerule_id) VALUES (?)", p)
        self.db.commit()
        cursor.close()

    def add_line_rule(self, line_rule):
        """Adds a line_rule to the database
        
        Keyword arguments:
        line_rule -- models.LineRule object
        
        """
        assert isinstance(line_rule, models.LineRule)

        cursor = self.db.cursor()
        p = [line_rule.get_name()]
        if line_rule.get_path_rule() is not None:
            if line_rule.get_path_rule().get_id() is None:
                self.add_path_rule(line_rule.get_path_rule())
            p.append(line_rule.get_path_rule().get_id())
        
        else:
            p.append(None)
        cursor.execute("INSERT INTO line_rule (name, path_rule_id) VALUES (?, ?)", p)
        self.db.commit()
        cursor.execute("SELECT last_insert_rowid() AS id")
        line_rule.set_id(cursor.fetchone()["id"])
        cursor.close()
        for r in line_rule.get_rerules():
            self.add_line_rule_rerule(line_rule, r)
    
    def add_line_rule_rerule(self, line_rule, rerule):
        """Adds a line rule rerule to the database.
        
        Keyword arguments:
        line_rule -- models.LineRule object
        rerule -- models.RERule object
        
        """
        assert isinstance(line_rule, models.LineRule)
        assert isinstance(rerule, models.RERule)

        if rerule.get_id() is None:
            self.add_rerule(rerule)
        cursor = self.db.cursor()
        p = (line_rule.get_id(), rerule.get_id())
        cursor.execute("INSERT INTO line_rule_rerule (line_rule_id, rerule_id) VALUES (?, ?)", p)
        self.db.commit()
        cursor.close()

    def add_rerule(self, rerule):
        """Adds a rerule object to the database.
        
        Keyword arguments:
        rerule -- models.RERule object
        """
        assert isinstance(rerule, models.RERule)

        cursor = self.db.cursor()
        if rerule.get_prepattern() is not None:
            if rerule.get_prepattern().get_id() is None:
                self.add_rerule(rerule.get_prepattern())
            p = (rerule.get_name(), rerule.get_pattern().get_id(), rerule.get_extract(), rerule.get_apply(), rerule.get_pattern())
            cursor.execute("INSERT INTO rerule (name, prepattern_id, extract, apply, pattern) VALUES (?, ?, ?, ?, ?)", p)
            self.db.commit()
        else:
            p = (rerule.get_name(), rerule.get_extract(), rerule.get_apply(), rerule.get_pattern())
            cursor.execute("INSERT INTO rerule (name, extract, apply, pattern) VALUES (?, ?, ?, ?)", p)
            self.db.commit()
        cursor.execute("SELECT last_insert_rowid() AS id")
        rerule.set_id(cursor.fetchone()["id"])
        cursor.close()

        # Save child elements
        for constant in rerule.get_constants():
            self.add_rerule_constant(rerule, constant)
        for tag in rerule.get_tags():
            self.add_rerule_tag(rerule, tag)
        for template in rerule.get_templates():
            self.add_rerule_template(rerule, template)
        for rewrite in rerule.get_rewrites():
            self.add_rerule_rewrite(rerule, rewrite)

    def add_rerule_constant(self, rerule, constant):
        """Adds a rerule constant to the database.
        
        Keyword arguments:
        rerule -- models.RERule object
        constant -- models.RERuleConstant object
        """
        assert isinstance(rerule, models.RERule)
        assert isinstance(constant, models.RERuleConstant)

        p = [rerule.get_id(), constant.get_constant_name()]
        cursor = self.db.cursor()
        cursor.execute("SELECT id FROM rerule_constant WHERE rerule_id=? AND constant_name=?", p)
        r = cursor.fetchone()
        if r is None:
            p.append(constant.get_constant_value())
            cursor.execute("INSERT INTO rerule_constant (rerule_id, constant_name, constant_value) VALUES (?, ?, ?)", p)
            self.db.commit()
            cursor.execute("SELECT last_insert_rowid() AS id")
            constant.set_id(cursor.fetchone()["id"])
        else:
            constant.set_id(r["id"])
        cursor.close()
    
    def add_rerule_tag(self, rerule, tag):
        """Adds a rerule tag to the database.
        
        keyword arguments:
        rerule -- models.RERule object
        tag -- models.RERuleTag object
        """
        assert isinstance(rerule, models.RERule)
        assert isinstance(tag, models.RERuleTag)

        p = (rerule.get_id(), tag.get_tag_name())
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO rerule_tag (rerule_id, tag_name) VALUES (?, ?)", p)
        self.db.commit()
        cursor.execute("SELECT last_insert_rowid() AS id")
        tag.set_id(cursor.fetchone()["id"])
        cursor.close()
    
    def add_rerule_template(self, rerule, template):
        """Adds a rerule template to the database.
        
        Keyword arguments:
        rerule -- models.RERule object
        template -- models.RERuleTemplate object
        """
        assert isinstance(rerule, models.RERule)
        assert isinstance(template, models.RERuleTemplate)

        p = (rerule.get_id(), template.get_template())
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO rerule_template (rerule_id, template) VALUES (?, ?)", p)
        cursor.execute("SELECT last_insert_rowid() AS id")
        template.set_id(cursor.fetchone()["id"])
        self.db.commit()
        cursor.close()

    def add_rerule_rewrite(self, rerule, rewrite):
        """Adds a rerule rewrite to the database.
        
        Keyword arguments:
        rerule -- models.RERule object
        rewrite -- models.RERuleRewrite object
        """
        assert isinstance(rerule, models.RERule)
        assert isinstance(rewrite, models.RERuleRewrite)

        p = (rerule.get_id(), rewrite.get_rewrite_pattern(), rewrite.get_rewrite_template())
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO rerule_rewrite (rerule_id, rewrite_pattern, rewrite_template) VALUES (?, ?, ?)", p)
        self.db.commit()
        cursor.execute("SELECT last_insert_rowid() AS id")
        rewrite.set_id(cursor.fetchone()["id"])
        cursor.close()

    def add_path_rule_to_outbox(self, outbox, path_rule):
        """Adds a path rule to the database and appends it to the outbox object.
        
        Keyword arguments:
        outbox -- models.Outbox object
        path_rule -- models.PathRule object
        """
        assert isinstance(outbox, models.Outbox)
        assert isinstance(path_rule, models.PathRule)

        self.add_path_rule(path_rule)
        cursor = self.db.cursor()
        p = (outbox.get_id(), path_rule.get_id())
        cursor.execute("INSERT INTO outbox_path_rule (outbox_id, path_rule_id) VALUES (?, ?)", p)
        outbox.add_path_rule(path_rule)
        self.db.commit()
        cursor.close()

    def add_line_rule_to_outbox(self, outbox, line_rule):
        """Adds a line rule to the database and appends it to the outbox object.
        
        Keyword arguments:
        outbox -- models.Outbox object
        line_rule -- moddels.LineRule object
        
        """
        assert isinstance(outbox, models.Outbox)
        assert isinstance(line_rule, models.LineRule)

        self.add_line_rule(line_rule)
        cursor = self.db.cursor()
        p = (outbox.get_id(),line_rule.get_id())
        cursor.execute("INSERT INTO outbox_line_rule (outbox_id, line_rule_id) VALUES (?, ?)", p)
        outbox.add_line_rule(line_rule)
        self.db.commit()
        cursor.close()
        
    def find_outbox_path_rules(self, outbox):
        """Returns all of the path rules assigned to the outbox.
        
        Keyword arguments:
        outbox -- models.Outbox object
        
        """
        assert isinstance(outbox, models.Outbox)

        path_rules = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT r.id, r.name, r.prepattern_id, r.pattern, r.extract, r.apply FROM outbox_path_rule AS o INNER JOIN path_rule AS p ON (o.path_rule_id=p.rerule_id) INNER JOIN rerule AS r ON (p.rerule_id=r.id) WHERE o.outbox_id=?", p)
        result = cursor.fetchall()
        cursor.close()
        for r in result:
            pr = models.PathRule(**r)
            # don't join this field in case it is too recursive
            if r["prepattern_id"] is not None:
                pr.set_prepattern(self.find_rerule_by_id(r["prepattern_id"]))
            self._populate_rerule_associations(pr)
            path_rules.append(pr)
        
        return path_rules
    
    def find_rerule_by_id(self, rerule_id):
        """Retrieves a rerule object given its id, returns None if not found.
        
        Keyword arguments:
        rerule_id -- the id of the rerule.
        
        """
        rerule = None
        cursor = self.db.cursor()
        p = (rerule_id,)
        cursor.execute("SELECT r.id, r.name, r.prepattern_id, r.pattern, r.extract, r.apply FROM rerule AS r WHERE r.id=?", p)
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            rerule = models.RERule(**r)
            if r["prepatter_id"] is not None:
                rerule.set_prepattern(self.find_rerule_by_id(r["prepattern_id"]))
        return rerule

    def find_path_rule_by_id(self, path_rule_id):
        path_rule = None
        cursor = self.db.cursor()
        p = (path_rule_id,)
        cursor.execute("SELECT r.id, r.name, r.prepattern_id, r.pattern, r.extract, r.apply FROM path_rule AS p INNER JOIN rerule AS r ON (p.rerule_id=r.id) WHERE p.rerule_id=?", p)
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            path_rule = models.PathRule(**r)
            if r["prepattern_id"] is not None:
                path_rule.set_prepattern(self.find_rerule_by_id(r["prepattern_id"]))
        return path_rule

    def find_outbox_line_rules(self, outbox):
        """Returns all of the line rules assigned to the outbox.
        
        Keyword arguments:
        outbox -- models.Outbox object
        """
        assert isinstance(outbox, models.Outbox)

        line_rules = []
        cursor = self.db.cursor()
        p = (outbox.get_id(),)
        cursor.execute("SELECT l.id, l.name, l.path_rule_id FROM outbox_line_rule AS o INNER JOIN line_rule AS l ON (o.line_rule_id=l.id) WHERE o.outbox_id=?", p)
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            lr = models.LineRule(**r)
            # don't join this field in case it is too recursive
            if r["path_rule_id"] != None:
                lr.set_path_rule(self.find_path_rule_by_id(r["path_rule_id"]))
            lr.set_rerules(self.find_line_rule_rerules(lr))
            line_rules.append(lr)
        return line_rules
    
    def find_line_rule_rerules(self, line_rule):
        """Returns all of the rerules associated with a line rule.
        
        Keyword arguments:
        line_rule -- models.LineRule object
        
        """
        assert isinstance(line_rule, models.LineRule)

        rerules = []
        cursor = self.db.cursor()
        p = (line_rule.get_id(),)
        cursor.execute("SELECT r.id, r.name, r.prepattern_id, r.extract, r.apply FROM line_rule_rerule AS l INNER JOIN rerule AS r ON (l.rerule_id=r.id) WHERE l.line_rule_id=?", p)
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            rerule = models.RERule(**r)
            if r["prepattern_id"] is not None:
                rerule.set_prepattern(self.find_rerule_by_id(r["prepattern_id"]))
            self._populate_rerule_associations(rerule)
            rerules.append(rerule)
        return rerules

    def _populate_rerule_associations(self, rerule):
        rerule.set_tags(self.find_rerule_tags(rerule))
        rerule.set_templates(self.find_rerule_templates(rerule))
        rerule.set_constants(self.find_rerule_constants(rerule))
        rerule.set_rewrites(self.find_rerule_rewrites(rerule))

    def find_rerule_tags(self, rerule):
        """Returns all of the tags assigned to a rerule.
        
        Keyword arguments:
        rerule -- models.RERule object
        
        """
        assert isinstance(rerule, models.RERule)

        tags = []
        cursor = self.db.cursor()
        p = (rerule.get_id(),)
        cursor.execute("SELECT id, rerule_id, tag_name FROM rerule_tag WHERE rerule_id = ?", p)
        for r in cursor.fetchall():
            tags.append(models.RERuleTag(**r))
        cursor.close()
        return tags

    def find_rerule_templates(self, rerule):
        """Returns all of the templates assigned to a rerule.
        
        Keyword arguments:
        rerule - models.RERule object
        
        """
        assert isinstance(rerule, models.RERule)

        templates = []
        cursor = self.db.cursor()
        p = (rerule.get_id(),)
        cursor.execute("SELECT id, rerule_id, template FROM rerule_template WHERE rerule_id=?", p)
        for r in cursor.fetchall():
            templates.append(models.RERuleTemplate(**r))
        cursor.close()
        return templates

    def find_rerule_constants(self, rerule):
        """Returns all of the constants assigned to a rerule
        
        Keyword arguments:
        rerule -- models.RERule object
        
        """
        assert isinstance(rerule, models.RERule)
        constants = []
        cursor = self.db.cursor()
        p = (rerule.get_id(),)
        cursor.execute("SELECT id, rerule_id, constant_name, constant_value FROM rerule_constant WHERE rerule_id=?", p)
        for r in cursor.fetchall():
            constants.append(models.RERuleConstant(**r))
        cursor.close()
        return constants
    
    def find_rerule_rewrites(self, rerule):
        """Returns all rewrites assigned to a rerule
        
        Keyword arguments:
        rerule -- models.RERule object
        
        """
        assert isinstance(rerule, models.RERule)
        rewrites = []
        cursor = self.db.cursor()
        p = (rerule.get_id(),)
        cursor.execute("SELECT id, rerule_id, rewrite_pattern, rewrite_template FROM rerule_rewrite WHERE rerule_id=?", p)
        for r in cursor.fetchall():
            rewrites.append(models.RERuleRewrite(**r))
        cursor.close()
        return rewrites
    
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
        state = models.ScanState()
        state.set_state(state_name)
        cursor = self.db.cursor()
        p = (state_name,)
        cursor.execute("SELECT id FROM scan_state WHERE state=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO scan_state (state) VALUES (?)", p)
            self.db.commit()
            cursor.execute("SELECT last_insert_rowid() AS id")
            state.set_id(cursor.fetchone()['id'])
        else:
            state.set_id(r['id'])
        cursor.close()
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
            scans.append(models.Scan(**r))
        return scans
    
    def start_file_scan(self):
        """Adds a new scan and returns a scan object.
        
        """
        start_state = self.find_scan_state('START_FILE_SCAN')
        cursor = self.db.cursor()
        p = (time.time(), start_state.get_id())
        cursor.execute("INSERT INTO scan (start, scan_state_id) VALUES (?, ?)", p)
        self.db.commit()
        cursor.execute("SELECT last_insert_rowid() AS id")
        p = (cursor.fetchone()['id'],)
        cursor.execute("SELECT s.id, s.start, s.end, s.scan_state_id, st.state FROM scan AS s INNER JOIN scan_state AS st ON (s.scan_state_id=st.id) WHERE s.id=?", p)
        r = cursor.fetchone()
        cursor.close()
        return models.Scan(**r)

    def find_last_scan(self):
        """Retrieves the last scan that was started.
        
        """
        scan = None
        cursor = self.db.cursor()
        cursor.execute("SELECT s.id, s.start, s.end, s.scan_state_id, st.state FROM scan AS s INNER JOIN scan_state AS st ON (s.scan_state_id=st.id) ORDER BY s.end DESC LIMIT 1")
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            scan = models.Scan(**r)
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
            f = models.File(**r)
        return f
    
    def add_file(self, f):
        """Adds a new file object to the database.
        
        Keyword arguments:
        f -- models.File object
        
        """
        assert isinstance(f, models.File)

        cursor = self.db.cursor()
        p = (f.get_filepath(),)
        cursor.execute("SELECT id FROM file WHERE filepath=?", p)
        r = cursor.fetchone()
        if r is None:
            p = (f.get_filepath(), f.get_mtime(), f.get_size(), f.get_checksum(), f.get_must_tag())
            cursor.execute("INSERT INTO file (filepath, mtime, size, checksum, must_tag) VALUES (?, ?, ?, ?, ?)", p)
            self.db.commit()
            cursor.execute("SELECT last_insert_rowid() AS id")
            f.set_id(cursor.fetchone()["id"])
        else:
            f.set_id(r["id"])
        cursor.close()
        return f

    def update_file(self, f):
        """Saves information from an existing file object to the database.
        
        Keyword arguments:
        f -- models.File object
        
        """
        assert isinstance(f, models.File)

        cursor = self.db.cursor()
        p = (f.get_filepath(), f.get_mtime(), f.get_size(), f.get_checksum(), f.get_must_tag(), f.get_id())
        cursor.execute("UPDATE file SET filepath=?, mtime=?, size=?, checksum=?, must_tag=? WHERE id=?", p)
        self.db.commit()
        cursor.close()

    def add_file_to_scan(self, scan, f):
        """Adds an existing file to an existing scan in the database.
        
        Keyword arguments:
        scan -- models.Scan object
        file -- models.File object
        
        """
        assert isinstance(scan, models.Scan)
        assert isinstance(f, models.File)

        if f.get_id() is None:
            self.add_file(f)
        cursor = self.db.cursor()
        p = (scan.get_id(), f.get_id())
        cursor.execute("SELECT 1 FROM scan_files WHERE scan_id=? AND file_id=?", p)
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO scan_files (scan_id, file_id) VALUES (?, ?)", p)
            self.db.commit()
        cursor.close()

    def find_files_in_scan(self, scan):
        """Returns all files associated with a particular scan.
        
        Keyword arguments:
        scan -- models.Scan object
        
        """
        assert isinstance(scan, models.Scan)

        cursor = self.db.cursor()
        p = (scan.get_id(),)
        files = []
        cursor.execute("SELECT f.id, f.filepath, f.mtime, f.size, f.checksum, f.must_tag FROM scan_files AS s INNER JOIN file AS f ON (s.file_id=f.id) WHERE s.scan_id=?", p)
        for r in cursor.fetchall():
            files.append(models.File(**r))
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
        self.db.commit()
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
            scan = models.Scan(**r)
            scan.set_files(self.find_files_in_scan(scan))
            scans.append(scan)
        cursor.close()
        return scans
    
    def register_file(self, f):
        """Registers a file to be added to tagfiler.
        
        Keyword arguments:
        f -- models.File object to register
        
        """
        assert isinstance(f, models.File)

        registered_file = models.RegisterFile()
        registered_file.set_file(f)
        cursor = self.db.cursor()
        p = (f.get_id(),)
        cursor.execute("SELECT id FROM register_file WHERE file_id=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO register_file (file_id, added) VALUES (?, date('now'))", p)
            self.db.commit()
            cursor.execute("SELECT last_insert_rowid() AS id")
            registered_file.set_id(cursor.fetchone()["id"])
        else:
            registered_file.set_id(r["id"])
        cursor.close()
        return registered_file

    def add_registered_file_tag(self, register_file, tag):
        """Adds a tag to include in registering a file.
        
        Keyword arguments:
        register_file -- models.RegisterFile object
        tag -- models.RegisterTag object to include
        
        """
        assert isinstance(register_file, models.RegisterFile)
        assert isinstance(tag, models.RegisterTag)

        cursor = self.db.cursor()
        p = (register_file.get_id(), tag.get_tag_name(), tag.get_tag_value())
        cursor.execute("SELECT id FROM register_tag WHERE register_file_id=? AND tag_name=? AND tag_value=?", p)
        r = cursor.fetchone()
        if r is None:
            cursor.execute("INSERT INTO register_tag (register_file_id, tag_name, tag_value) VALUES (?, ?, ?)", p)
            self.db.commit()
            cursor.execute("SELECT last_insert_rowid() AS id")
            tag.set_id(cursor.fetchone()["id"])
        else:
            tag.set_id(r["id"])
        cursor.close()

    def find_tagged_files_to_register(self):
        """Retrieves a list of all files to register.
        
        """
        files = []
        cursor = self.db.cursor()
        cursor.execute("SELECT r.id AS register_file_id, r.added, f.id, f.filepath, f.mtime, f.size, f.checksum, f.must_tag FROM register_file AS r INNER JOIN file AS f ON (r.file_id=f.id) WHERE f.must_tag='FALSE'")
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            f = models.RegisterFile(**r)
            f.set_tags(self.find_tags_to_register(f))
            files.append(f)
        return files

    def find_tags_to_register(self, register_file):
        """Retrieves a list of all tags to include for a file to register
        
        Keyword arguments:
        register_file -- models.RegisterFile object
        
        """
        assert isinstance(register_file, models.RegisterFile)
        tags = []
        cursor = self.db.cursor()
        p = (register_file.get_id(),)
        cursor.execute("SELECT id, register_file_id, tag_name, tag_value FROM register_tag WHERE register_file_id=?", p)
        results = cursor.fetchall()
        cursor.close()
        for r in results:
            tags.append(models.RegisterTag(**r))
        return tags

    def remove_registered_file_and_tags(self, registered_file):
        """Removes the registered file and its tags from the database.
        
        Keyword arguments:
        registered_file -- models.RegisterFile to remove
        
        """
        assert isinstance(registered_file, models.RegisterFile)
        p = (registered_file.get_id(),)
        cursor = self.db.cursor()
        cursor.execute("DELETE FROM register_tag WHERE register_file_id=?", p)
        cursor.execute("DELETE FROM register_file WHERE id=?", p)
        self.db.commit()
        registered_file.set_id(None)
