
import ftree
import rules
import urllib
import urlparse
import Cookie
from httplib import HTTPConnection, HTTPSConnection, HTTPException, OK, CREATED, ACCEPTED, NO_CONTENT
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

def load_treescan_stats_sha256(db, topid, era, excludes=[], includes=[], use_full_path=False):
    """Load treescan table from filesystem content.

       Behaves much like ftree.tree_scan_stats_sha256(...) except the
       database table 'treescan' stores old and new scan results
       rather than generating a sequenserializablece. Old scans are used to avoid
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

        if use_full_path is True:
            rfpath = fpath
        
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
    return list(db.query('SELECT c.topid, c.connid, s.scanid,'
                    + '     s.rfpath, s.size, s.mtime, s.user, s."group", s.sha256sum,'
                    + '     t.subject, t.name, t.tagera, t.xferpos'
                    + ' FROM connections AS c'
                    + ' JOIN treescan AS s ON (c.topid=s.topid)'
                    + ' LEFT OUTER JOIN transfers AS t ON (c.connid=t.connid AND s.scanid=t.scanid)'
                    + ' WHERE c.topid = $topid'
                    + '   AND s.scanera2 = $scanera'
                    + '   AND ((s.size IS NOT NULL AND c.filemode IS NOT NULL) OR (s.size IS NULL AND c.dirmode IS NOT NULL))'
                    + '   AND (t.subject IS NULL'
                    + '        OR (s.size IS NOT NULL AND t.xferpos < s.size)'
                    + '        OR (t.tagera IS NOT NULL AND t.tagera < c.ruleera))'
                    + ' ORDER BY c.connid, s.scanid', 
                    vars=dict(topid=topid, scanera=era)))

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
        self.client = TagfilerServiceClient(self.peer.baseuri, self.peer.username, self.peer.password)

    def analyze(self, workitem):
        """Determine tag-values for this workitem."""
        # compute tags and append as subject dictionary to self.table
        self.table.append( rules.apply_rules(self.rules, self.top, workitem.rfpath, isfile=(workitem.size != None)) )
        self.tagnames.update( set(self.table[-1].keys()) )
        return self.table[-1]['name']

    def finish(self):
        """Finish any actions pertaining to connection."""
        # create JSON version of self.table and send to peer in PUT /subject/name(...) bulk registration
        try:
            self.tagnames.remove('name')
        except KeyError:
            pass

        # convert every attribute to a list so that it is serializable
        parsed_table = []
        for entry in self.table:
            parsed_dict = {}
            for (k,v) in entry.iteritems():
                parsed_dict[k] = list(v)
            parsed_table.append(parsed_dict)
        payload = jsonWriter(parsed_table)
        bulkurl = '/tagfiler/subject/name(%s)' % ';'.join([ urllib.quote(tag) for tag in self.tagnames ])
        login_cookie = self.client.send_login_request()
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Cookie"] = login_cookie
        self.client.send_request("PUT", bulkurl, payload, headers)

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
        num_results = self.db.query('SELECT COUNT(*) AS count FROM transfers WHERE connid = $connid AND scanid = $scanid', vars=vars)[0]["count"]
        if num_results == 0:
            vars['name'] = list(self.analyze(workitem))[0]
            self.db.query('INSERT INTO transfers (connid, scanid, name) VALUES ($connid, $scanid, $name)', vars=vars)
        elif (not workitem.name) or (not workitem.tagera) or (workitem.tagera < self.connection.ruleera):
            name = self.analyze(workitem)
            if workitem.name and workitem.name != name:
                pass
            
        if workitem.size != None and self.connection.filemode == 'upload':
            # file item needs independent upload
            self.upload_file(workitem)
        
class TagfilerServiceClient(object):

    def __init__(self, baseuri, username, password):
        self.baseuri = baseuri
        o = urlparse.urlparse(self.baseuri)
        self.scheme = o[0]
        host_port = o[1].split(":")
        self.host = host_port[0]
        self.port = None
        if len(host_port) > 1:
            self.port = host_port[1]
        self.username = username
        self.password = password

    def send_request(self, method, url, body='', headers={}):
        
        webconn = None
        if self.scheme == 'https':
            webconn = HTTPSConnection(host=self.host, port=self.port)
        elif self.scheme == 'http':
            webconn = HTTPConnection(host=self.netloc, port=self.port)
        else:
            raise ValueError('Scheme %s is not supported.' % self.scheme)
        webconn.request(method, url, body, headers)
        resp = webconn.getresponse()
        if resp.status not in [OK, CREATED, ACCEPTED, NO_CONTENT]:
            raise HTTPException("Error response (%i) received: %s" % (resp.status, resp.read()))
        return resp

    def send_login_request(self):
        headers = {}
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        resp = self.send_request("POST", "/webauthn/login", "username=%s&password=%s" % (self.username, self.password), headers)
        return resp.getheader("set-cookie")

def process_worklist(db, topid, connid, era):
    conn = None

    for w in generate_worklist(db, topid, era):
        if not conn or w.connid != conn.connid:
            if conn:
                conn.finish()
            conn = Connection(db, connid, era)

        conn.perform(w)

    if conn:
        conn.finish()

def get_top_id(db, top, create=False):
    """
    Returns the ID of the requested top path in the DB.
    db: the database
    top: the directory path of the top
    create: True if an entry should be created if it doesn't exist (default is False)
    """
    topid = None
    vars=dict(top=top)
    try:
        topid = db.query("SELECT topid FROM tops WHERE top=$top", vars=vars)[0]["topid"]
    except IndexError,e:
        if create is True:
            db.query("INSERT INTO tops (top) VALUES ($top)", vars=vars)
            topid = db.query("SELECT last_insert_rowid() AS topid")[0]["topid"]
        else:
            raise e
    return topid

def get_current_scan_era(db, topid):
    """
    returns the current era in use from a treescan on a directory
    db: the database
    topid: the topid of the directory from the tops table
    """
    currentera = 0
    try:
        currentera = db.query("SELECT max(currentera) AS currentera FROM configstate")[0]["currentera"]
    except IndexError:
        # no era yet - use 0 as the start
        pass
    return currentera

def get_peer_id(db, peer_name, create=False, peer_url=None, peer_username=None, peer_password=None):
    """
	returns the peerid identified by a peer name
	db: the database
	peer_name: unique name assigned to the peer
	create: create the peer if it doesn't exist (default is false)
	peer_url: tagfiler url
	peer_username: tagfiler username
	peer_password: tagfiler password
	"""
    peerid = None
    vars=dict(peer=peer_name)
    try:
        peerid = db.query("SELECT peerid FROM peers WHERE peer=$peer", vars=vars)[0]["peerid"]
    except IndexError:
        if create is True:
            vars=dict(peer=peer_name, baseuri=peer_url, username=peer_username, password=peer_password)
            db.query("INSERT INTO peers (peer, baseuri, username, password) VALUES ($peer, $baseuri, $username, $password)", vars=vars)
            peerid = db.query("SELECT last_insert_rowid() AS peerid")[0]["peerid"]
    return peerid

def get_conn_id(db, topid, peerid, ruleera, create=False):
    connid = None
    vars = dict(topid=topid, peerid=peerid)
    try:
        connid = db.query("SELECT connid FROM connections WHERE topid=$topid AND peerid=$peerid", vars=vars)[0]['connid']
    except IndexError:
        if create is True:
            vars=dict(topid=topid, peerid=peerid, ruleera=ruleera, filemode='True', dirmode='')
            db.query("INSERT INTO connections (topid, peerid, ruleera, filemode, dirmode) VALUES ($topid, $peerid, $ruleera, $filemode, $dirmode)", vars=vars)
            connid = db.query("SELECT last_insert_rowid() AS connid")[0]["connid"]
    return connid

def get_rule_id(db, connid, ruletype, ruledata, create=True):
    vars=dict(ruletype=ruletype, ruledata=jsonWriter(ruledata), connid=connid)
    try:
        rowid = db.query("SELECT rowid FROM rules WHERE connid=$connid AND ruletype=$ruletype AND ruledata=$ruledata", vars=vars)[0]["rowid"]
    except IndexError:
        if create is True:
            db.query("INSERT INTO rules (connid, ruletype, ruledata) VALUES ($connid, $ruletype, $ruledata)", vars=vars)
            rowid = db.query("SELECT last_insert_rowid() AS rowid", vars=vars)[0]["rowid"]
    return rowid

