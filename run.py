"""
This file is meant to prototype the use of tagging rules on a local
system to bulk tag new files into TagFiler.
"""
try:
    import sqlite3
except:
    import pysqlite2.dbapi2
import web
import sys

from settings import DATABASE_FILE, TAGFILER_BASEURL, TAGFILER_PEER_NAME, TAGFILER_USERNAME, TAGFILER_PASSWORD
from dbinit import init_db
from iobox import *
import globus_online_rules

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "Usage: %s <tree root> <endpoint_name>" % sys.argv[0]
        sys.exit(1)
    tree_root = sys.argv[1]
    endpoint_name = sys.argv[2]

    tag_rules = globus_online_rules.generate_rules(endpoint_name)
    # Create database connection
    print "Connecting to database %s..." % DATABASE_FILE
    db = web.database(db=DATABASE_FILE, dbn='sqlite')
    print "Connected successfully!"

    print "Initializing database..."
    init_db(db)
    print "Database initialized successfully!"

    print "Looking up tree_root in tops table..."
    topid = get_top_id(db, tree_root, create=True)
    print "Using topid=%i" % topid

    print "Looking up current era..."
    currentera = get_current_scan_era(db, topid)
    print "currentera=%i" % currentera

    peerid = get_peer_id(db, peer_name=TAGFILER_PEER_NAME, create=True, peer_url=TAGFILER_BASEURL, peer_username=TAGFILER_USERNAME, peer_password=TAGFILER_PASSWORD)
    connid = get_conn_id(db, topid=topid, peerid=peerid, ruleera=currentera, create=True)

    # tag any files that haven't been processed in current era (must have been interrupted
    process_worklist(db=db, topid=topid, connid=connid, era=currentera)

    # start new era session
    currentera += 1
    print "Starting new era %i" % currentera

    print "Scanning %s using era=%i..." % (tree_root, currentera)
    load_treescan_stats_sha256(db, topid, currentera, use_full_path=True)
    print "Scan complete!"

    # start tagging
    for (ruletype, ruledata) in tag_rules:
        get_rule_id(db, connid=connid, ruletype=ruletype, ruledata=ruledata, create=True)

    # process worklist of items to tag
    process_worklist(db=db, topid=topid, connid=connid, era=currentera)
    print "Finished processing worklist"
