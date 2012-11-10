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
The data access object (DAO) class definitions.
"""

import sqlite3
import logging
import os
import models


logger = logging.getLogger(__name__)


class DataDAO(object):
   
    def __init__(self, db_filename, sql_filename):
        """Constructs a DAO instance, creating the database schema in the 
        database file if necessary
        
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
        
        # Test for existence of the state db, before issuing the connect
        db_exists = os.path.exists(self.db_filename)
        self.db = sqlite3.connect(self.db_filename, detect_types=sqlite3.PARSE_DECLTYPES)
        self.db.row_factory = _dict_factory
        
        # If db didn't exist (prior to sqlite connect), create database schema
        if not db_exists:
            logger.info("Storing local state in %s." % self.db_filename)
            
            import tagfiler.iobox
            sql_source_dir = os.path.join(os.path.dirname(tagfiler.iobox.__file__), "sql/")
            source_file = os.path.join(sql_source_dir, sql_filename)
            
            cursor = self.db.cursor()
            f = open(source_file, "r")
            sql_stmts = str.split(f.read(), ";")
            for s in sql_stmts:
                logger.debug("Executing statement %s" % s)
                s.strip()
                if len(s) > 0:
                    cursor.execute(s)
            f.close()
            cursor.close()
    
    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None


class OutboxStateDAO(DataDAO):
    """Data Access Object for a particular outbox's state."""
    
    def __init__(self, db_filename):
        super(OutboxStateDAO, self).__init__(db_filename, "outbox_state.sql")
                
    
    def add_file(self, f):
        """Adds a new file object to the database."""
        p = (f.filename, f.mtime, f.size, f.checksum, f.username, f.groupname)
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO file (filename, mtime, size, checksum, username, groupname) VALUES (?, ?, ?, ?, ?, ?)", p)
        cursor.execute("SELECT last_insert_rowid() AS id")
        f.id = cursor.fetchone()["id"]
        cursor.close()
        self.db.commit()

    def update_file(self, f):
        """Updates a file entry in the database."""
        p = (f.filename, f.mtime, f.rtime, f.size, f.checksum, f.username, f.groupname, f.id)
        cursor = self.db.cursor()
        cursor.execute("UPDATE file SET filename = ?, mtime = ?, rtime = ?, size = ?, checksum = ?, username = ?, groupname = ? WHERE id = ?", p)
        cursor.close()
        self.db.commit()
    
    def find_file(self, filename):
        """Retrieves a file object from the database matching the filename."""
        f = None
        cursor = self.db.cursor()
        p = (filename,)
        cursor.execute("SELECT id, filename, mtime, rtime, size, checksum, username, groupname FROM file WHERE filename=?", p)
        r = cursor.fetchone()
        cursor.close()
        if r is not None:
            f = models.File(**r)
        return f
