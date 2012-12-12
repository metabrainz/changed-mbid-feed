#!/usr/bin/python

import sys
import os
from changedmbidfeed import config 
from changedmbidfeed import feed

# optparse is retarded and argparse is not support on our target platform, so we'll just it manually

if len(sys.argv) == 1:
    # Continue where we left off last time
    seq, timestamp = feed.read_state_data()
    if not seq:
        print "Cannot continue. Can't read state data file."
        sys.exit(-1)

    print "Last replication sequence: %d on %s" % (seq, timestamp)
    feed.generate_entry(config.OUTPUT_DIR, seq, timestamp)

if len(sys.argv) == 2 and (sys.argv[1] == '-c' or sys.argv[1] == '--create'):
    # check to make sure we dont already have a last updated file
    seq, timestamp = feed.read_state_data()
    if seq:
        print "A %s file already exists. If you would like to start a new feed, remove this file first." % feed.LAST_UPDATED_FILE
        sys.exit(-1)

    seq, timestamp = feed.get_current_replication_info()
    print "The database has replication sequence %s and timestamp %s" % (seq, timestamp)

    if not feed.save_state_data(seq, timestamp):
        print "Failed to save state data to %s" % feed.LAST_UPDATED_FILE
        print "Abort"
        sys.exit(-1)

    print "The timestamp for the last replication packet has been saved. The\n" + \
          "feed should now be set up. Call this script once an hour\n"          + \
          "after a new replication packet is available to start dumping\n"      + \
          "feed packets.\n"
