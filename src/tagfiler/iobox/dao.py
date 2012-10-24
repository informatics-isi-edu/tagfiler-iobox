# 
# Copyright 2010 University of Southern California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
The data access object (DAO) module.
"""

import sqlite3
import logging
import os
import time
import models


logger = logging.getLogger(__name__)


class DataDAO(object):
   
    def __init__(self, db_filename, sql_filename):
        """Constructs a DAO instance, creating the database schema in the database file if necessary
        
        The 'db_filename' parameter is the filename of the database (a sqlite3 
        database).
        
        The 'sql_filename' parameter is the filename of the SQL DDL script to 
        be used in order to create the corresponding database.
        """
        def _dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
    
        self.db_filename = db_filename
        
        # Test for existence of the state db
        db_exists = os.path.exists(self.db_filename)
            
        # TODO(schuler): sort out the appropriate sqlite3 connection parameters:
        #self.db = sqlite3.connect(self.db_filename, check_same_thread = False, isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES)
        self.db = sqlite3.connect(self.db_filename, detect_types=sqlite3.PARSE_DECLTYPES)
        self.db.row_factory = _dict_factory
        
        # If db didn't exist (prior to sqlite connect), create database schema
        if db_exists:
            logger.info("Storing local state in %s." % self.db_filename)
            
            import tagfiler.iobox
            sql_source_dir = os.path.join(os.path.dirname(tagfiler.iobox.__file__), "sql/")
            source_file = os.path.join(sql_source_dir, sql_filename)
            
            cursor = self.db.cursor()
            f = open(source_file, "r")
            sql_stmts = str.split(f.read(), ";")
            for s in sql_stmts:
                logger.debug("Executing statement %s" % s)
                cursor.execute(s)
            f.close()
            cursor.close()


    def __del__(self):
        self.close()

    
    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None

    '''
    def _create_db_from_source(self, db, source_file):
            cursor = db.cursor()
            f = open(source_file, "r")
            sql_stmts = str.split(f.read(), ";")
            for s in sql_stmts:
                logger.debug("Executing statement %s" % s)
                cursor.execute(s)
            f.close()
            cursor.close()
    '''

class OutboxStateDAO(DataDAO):
    """Data Access Object for a particular outbox's state."""
    
    def __init__(self, db_filename):
        super(OutboxStateDAO, self).__init__(db_filename, "outbox_state.sql")
                

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
        cursor.execute("SELECT s.id, s.start, s.end, s.scan_state_id, st.state FROM scan AS s INNER JOIN scan_state AS st ON (s.scan_state_id=st.id) ORDER BY s.start DESC LIMIT 1")
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
        logger.debug("OutboxStateDAO:add_file: %s" % str(f))
        #cursor = self.db.cursor()
        #p = (f.get_filepath(),)
        #cursor.execute("SELECT id FROM file WHERE filepath=?", p)
        #r = cursor.fetchone()
        #cursor.close()
        #if r is None:
        p = (f.get_filepath(), f.get_mtime(), f.get_size(), f.get_checksum(), f.get_must_tag())
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO file (filepath, mtime, size, checksum, must_tag) VALUES (?, ?, ?, ?, ?)", p)
        
        cursor.execute("SELECT last_insert_rowid() AS id")
        f.set_id(cursor.fetchone()["id"])
        cursor.close()
        #else:
        #    f.set_id(r["id"])
        return f

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
            cursor.execute("INSERT INTO register_file (file_id, added) VALUES (?, datetime('now'))", p)
            
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
            
            cursor.execute("SELECT last_insert_rowid() AS id")
            tag.set_id(cursor.fetchone()["id"])
        else:
            tag.set_id(r["id"])
        cursor.close()

    def update_file(self, f):
        """Updates an existing file in the database.
        
        Keyword arguments:
        f -- models.File object to save
        """
        assert isinstance(f, models.File)
        logger.debug("OutboxStateDAO:update_file: %s" % str(f))
        p = (f.get_size(), f.get_mtime(), f.get_checksum(), f.get_must_tag(), f.get_id())
        cursor = self.db.cursor()
        cursor.execute("UPDATE file SET size=?, mtime=?, checksum=?, must_tag=? WHERE id=?", p)
        cursor.close()
        
    def find_files_to_tag(self):
        """
        Retrieves a list of all files that have been scanned but need to be tagged.
        
        """
        files = []
        cursor = self.db.cursor()
        cursor.execute("SELECT id, filepath, checksum, size, mtime, must_tag FROM file WHERE must_tag=1")
        results = cursor.fetchall()
        cursor.close()
        for result in results:
            files.append(models.File(**result))
        return files

    def find_tagged_files_to_register(self):
        """Retrieves a list of all files to register.
        
        """
        files = []
        cursor = self.db.cursor()
        cursor.execute("SELECT r.id AS register_file_id, r.added, f.id, f.filepath, f.mtime, f.size, f.checksum, f.must_tag FROM register_file AS r INNER JOIN file AS f ON (r.file_id=f.id) WHERE f.must_tag=0")
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
        
        registered_file.set_id(None)
