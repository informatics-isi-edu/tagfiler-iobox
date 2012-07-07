
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

def load_treescan_stats_sha256(db, topid, era, excludes=[], includes=[]):
    """Load treescan table from filesystem content.

       Behaves much like ftree.tree_scan_stats_sha256(...) except the
       database table 'treescan' stores old and new scan results
       rather than generating a sequence. Old scans are used to avoid
       recomputing sha256 when the size and mtime match. The numeric
       era (scan era) is set in the table for use by other query
       functions.

       Tree members present during this scan can be fetched via:

         SELECT * FROM treescan WHERE scanera2 = $scanera AND topid = $topid
    """
    top = db.query('SELECT top FROM tops WHERE topid = $topid', vars=dict(topid=topid))[0].top

    for r in ftree.tree_scan_stats(top, excludes, includes):
        rfpath, size, mtime, user, group = r
        fpath = '%s%s' % (top, rfpath)
        sha256sum = None

        vars = dict(topid=topid, rfpath=rfpath, size=size, mtime=mtime, user=user, group=group, scanera=era)

        results = db.query('SELECT * FROM treescan WHERE topid = $topid AND rfpath = $rfpath', vars=vars)
        try:
            cached = results[0]
            if cached.size != size or cached.mtime != mtime:
                sha256sum = ftree.sha256sum(fpath)
                if cached.size != size or cached.sha256sum != sha256sum:
                    cached = None
                    db.query('DELETE FROM treescan WHERE topid = $topid AND rfpath = $rfpath', vars=vars)
            else:
                sha256sum = cached.sha256sum
        except IndexError:
            # no cached row
            cached = None
            sha256sum = ftree.sha256sum(fpath)
            db.query('UPDATE treescan SET scanera2 = $scanera WHERE topid = $topid AND rfpath = $rfpath', vars=vars)

        # now, sha256sum is initialized and cached is only initialized if db cache is up to date
            
        if not cached:
            vars['sha256sum'] = sha256sum
            db.query('INSERT INTO treescan (topid, rfpath, size, mtime, user, "group", sha256sum, scanera1, scanera2)'
                     + ' VALUES ($topid, $rfpath, $size, $mtime, $user, $group, $sha256sum, $scanera, $scanera)',
                     vars=vars)


def generate_worklist(db, topid, era):
    """Generate sequence of workitems for which tagging analysis and/or upload is pending.

       Considers current scan-era members for which transfer is not
       already complete and up to date with tagera.
    """
    return db.query('SELECT c.topid AS topid, c.connid AS connid, s.scanid AS scanid'
                    + '     s.rfpath AS rfpath, s.size AS size, s.mtime AS mtime, s.user AS user, s."group" AS group, s.sha256sum AS sha256sum,'
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
                    vars=dict(topid=topid, scanera=era))

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
        self.table.append( rules.apply_rules(self.rules, self.top, workitem.rfpath, isfile=(workitem.size != None)) )
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

        if workitem.size != None:
            if not self.connection.filemode:
                # NULL filemode: ignore files
                return
        else:
            if not self.connection.dirmode:
                # NULL dirmode: ignore dirs
                return

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

       
