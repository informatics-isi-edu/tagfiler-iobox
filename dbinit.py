
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

