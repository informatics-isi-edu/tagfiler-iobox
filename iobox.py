
import ftree
import rules

try:
    import sqlite3
except:
    import pysqlite2.dbapi2

try:
    import simplejson
    
    jsonWriter = simplejson.dumps
    jsonReader = simplejson.loads
    jsonFileReader = simplejson.load
except:
    import json

    if hasattr(json, 'dumps'):
        jsonWriter = json.dumps
        jsonReader = json.loads
        jsonFileReader = json.load
    else:
        raise RuntimeError('Could not configure JSON library.')

def create_state(db):
    db.query('CREATE TABLE configstate (currentera INTEGER)')
    db.query('INSERT INTO configstate (currentera) VALUES (1)')

def create_tops(db):
    db.query('CREATE TABLE tops (topid INTEGER PRIMARY KEY, top text UNIQUE)')

def create_peers(db):
    db.query('CREATE TABLE peers (peerid INTEGER PRIMARY KEY, peer text UNIQUE, baseuri text UNIQUE, username text, password text')

def create_connections(db):
    """Create connections table.
    
       Primary key: connid
       Alternate keys: topid, peerid

       Connection-specific configuration:
         -- ruleera is most recent era in which rules were modified
         -- filemode is how to process files
            -- NULL means skip regular files
            -- 'register' means send abstract record
            -- 'upload' means send file body
         -- dirmode is how to process directories
            -- NULL means skip directories
            -- 'register' means send abstract record
    """
    db.query('CREATE TABLE connections (connid INTEGER PRIMARY KEY,'
             + ' topid INTEGER REFERENCES tops(topid),'
             + ' peerid INTEGER REFERENCES peers(peerid),'
             + ' ruleera INTEGER NOT NULL DEFAULT 1,'
             + ' filemode text,'
             + ' dirmode text,'
             + ' UNIQUE(topid, peerid))')

def create_rules(db):
    """Create rules table.
    
       Connection-specific rules:
          -- connid  of related connection
          -- ruletype   is keyword for specific supported rule class
          -- ruledata   is JSON encoded rule structure appropriate for ruletype constructor
    """
    db.query('CREATE TABLE rules (connid INTEGER REFERENCES connections (connid)'
             + ' ruletype text,'
             + ' ruledata text,'
             + ' UNIQUE (connid, ruletype, ruledata))')

def create_treescan(db):
    """Create table treescan.
    
       Primary key: scanid
       Alternate keys: topid, rfpath

       File properties: size, mtime, user, group, sha256sum

       Scan state: scanera1, scanera2
         -- scanera1 is first scan at which current file content was discovered
         -- scanera2 is latest scan at which current file content was discovered

       If a file was found previously but then disappeared, its
       scanera2 value will fall behind the current era.

    """
    db.query('CREATE TABLE treescan (scanid INTEGER PRIMARY KEY,'
             + ' topid text NOT NULL REFERENCES tops(topid), rfpath text NOT NULL,'
             + ' size INTEGER, mtime float8, user text, group text, sha256sum text,'
             + ' scanera1 INTEGER NOT NULL, scanera2 INTEGER NOT NULL,'
             + ' UNIQUE(topid, rfpath) )')
    #db.query('CREATE INDEX treescan_pkey_idx ON treescan (topid, rfpath)')

def create_transfers(db):
    """Create table transfers.

       Primary key: connid, scanid

       Transfer status: subject, xferpos, tagera
         -- name is subject name at remote catalog
         -- subject is subject id at remote catalog
            -- NULL if not created
         -- xferpos is transfer status for file bodies
            -- NULL for non-transfer scenarios, e.g. register-only or non-file subjects
            -- natural integer means max file byte pos transferred so far, for recovery
         -- tagera is era when tags were last formulated and pushed
            -- NULL when not tagged so far
    """
    db.query('CREATE TABLE transfers (connid INTEGER REFERENCES connections(connid),'
             + ' scanid INTEGER REFERENCES treescan(scanid),'
             + ' name text NOT NULL,'
             + ' subject INTEGER,'
             + ' tagera INTEGER, xferpos INTEGER,'
             + ' PRIMARY KEY(connid, scanid),'
             + ' UNIQUE (connid, name))')

def init_db(db):
    create_state(db)
    create_tops(db)
    create_peers(db)
    create_connections(db)
    create_rules(db)
    create_treescan(db)
    create_transfers(db)

def load_treescan_stats_sha256(db, topid, scanera, excludes=[], includes=[]):
    """Load treescan table from filesystem content.

       Behaves much like ftree.tree_scan_stats_sha256(...) except the
       database table 'treescan' stores old and new scan results
       rather than generating a sequence. Old scans are used to avoid
       recomputing sha256 when the size and mtime match. The numeric
       scanera (scan era) is set in the table for use by other query
       functions.

       Tree members present during this scan can be fetched via:

         SELECT * FROM treescan WHERE scanera2 = $scanera AND topid = $topid
    """
    top = db.query('SELECT top FROM tops WHERE topid = $topid', vars=dict(topid=topid))[0].top

    for r in ftree.tree_scan_stats(topid, excludes, includes):
        rfpath, size, mtime, user, group = r
        fpath = '%s%s' % (top, rfpath)
        sha256sum = None

        vars = dict(topid=topid, rfpath=rfpath, size=size, mtime=mtime, user=user, group=group, scanera=scanera)

        results = db.query('SELECT * FROM treescan WHERE topid = $topid AND rfpath = $rfpath', vars=vars)
        
        if len(results) > 0:
            cached = results[0]
            if cached.size != size or cached.mtime != mtime:
                sha256sum = ftree.sha256sum(fpath)
                if cached.size != size or cached.sha256sum != sha256sum:
                    cached = None
                    db.query('DELETE FROM treescan WHERE topid = $topid AND rfpath = $rfpath', vars=vars)
            else:
                sha256sum = cached.sha256sum
        else:
            cached = None
            sha256sum = ftree.sha256sum(fpath)
            db.query('UPDATE treescan SET scanera2 = $scanera WHERE topid = $topid AND rfpath = $rfpath', vars=vars)

        # now, sha256sum is initialized and cached is only initialized if db cache is up to date
            
        if not cached:
            vars['sha256sum'] = sha256sum
            db.query('INSERT INTO treescan (topid, rfpath, size, mtime, user, group, sha256sum, scanera1, scanera2)'
                     + ' VALUES ($topid, $rfpath, $size, $mtime, $user, $group, $sha256sum, $scanera, $scanera)',
                     vars=vars)


def generate_worklist(db, topid, scanera):
    """Generate sequence of workitems for which tagging analysis and/or upload is pending.

       Considers current scan-era members for which transfer is not
       already complete and up to date with tagera.
    """
    return db.query('SELECT c.topid AS topid, c.connid AS connid, s.scanid AS scanid'
                    + '     s.rfpath AS rfpath, s.size AS size, s.mtime AS mtime, s.user AS user, s.group AS group, s.sha256sum AS sha256sum,'
                    + '     t.subject AS subject, t.name AS name, t.tagera AS tagera, t.xferpos AS xferpos'
                    + ' FROM connections AS c'
                    + ' JOIN treescan AS s USING (topid)'
                    + ' LEFT OUTER JOIN transfers AS t USING (connid, scanid)'
                    + ' WHERE c.topid = $topid'
                    + '   AND s.scanera2 = $scanera'
                    + '   AND ((s.size IS NOT NULL AND c.filemode IS NOT NULL) OR (s.size IS NULL AND c.dirmode IS NOT NULL))'
                    + '   AND (t.subject IS NULL'
                    + '        OR (s.size IS NOT NULL AND t.xferpos < s.size)'
                    + '        OR (t.tagera IS NOT NULL AND t.tagera < c.ruleera))'
                    + ' ORDER BY connid, scanid', 
                    vars=dict(topid=topid, scanera=scanera))

class Connection:

    def __init__(self, db, connid, tagera):
        self.db = db
        self.connid = connid
        self.era = tagera
        self.table = []
        c = db.query('SELECT * FROM connections WHERE connid = $connid', vars=dict(connid=connid))[0]
        self.connection = c
        self.top = db.query('SELECT * FROM tops WHERE topid = $topid', vars=dict(topid=c.topid))[0]
        self.peer = db.query('SELECT * FROM peers WHERE peerid = $peerid', vars=dict(peerid=c.peerid))[0]
        self.rules = [ rules.rule(r.ruletype, jsonReader(r.ruledata))
                       for r in db.query('SELECT * FROM rules WHERE connid = $connid', vars=dict(connid=connid)) ]
        self.tagnames = set()

    def analyze(self, workitem):
        """Determine tag-values for this workitem."""
        # compute tags and append as subject dictionary to self.table
        self.table.append( rules.apply_rules(self.rules, self.top, workitem.rfpath) )
        self.tagnames.update( set(self.table[-1].keys()) )
        return self.table[-1]['name']

    def finish(self):
        """Finish any actions pertaining to connection."""
        # create JSON version of self.table and send to peer in PUT /subject/name(...) bulk registration
        self.tagnames.remove('name')
        payload = jsonWriter(self.table)
        bulkurl = '%s/tags/name(%s)' % ';'.join([ urlquote(tag) for tag in self.tagnames ])

    def upload_file(self, workitem):
        """Upload an individual file w/ entity body from disk."""
        pass

    def perform(self, workitem):
        """Perform workitem-specific actions."""
        assert workitem.connid == self.connid

        name = workitem.name

        # make sure there's a transfer table entry for this workitem (might be absent due to left-outer-join)
        vars = dict(connid=self.connid, scanid=workitem.scanid)
        results = self.db.query('SELECT * FROM transfers WHERE connid = $connid AND scanid = $scanid', vars=vars)
        if len(results) == 0:
            name = self.analyze(workitem)
            vars['name'] = name
            self.db.query('INSERT INTO transfers (connid, scanid, name) VALUES ($connid, $scanid, $name)', vars=vars)
        elif (not workitem.name) or (not workitem.tagera) or (workitem.tagera < self.connection.ruleera):
            name = self.analyze(workitem)
            if workitem.name and workitem.name != name:
                pass
            
        if workitem.size != None and self.connection.filemode == 'upload':
            # file item needs independent upload
            self.upload_file(workitem)
        

def process_worklist(db, topid, era):
    conn = None

    for w in generate_worklist(db, topid, era):
        if not conn or w.connid != conn.connid:
            if conn:
                conn.finish()
            conn = Connection(db, connid, era)

        conn.perform(workitem)

    if conn:
        conn.finish()

       
