#!/usr/bin/python

import sys
import os
from changedmbidfeed import config 
from changedmbidfeed import feed

if len(sys.argv) == 1:
    # Continue where we left off last time
    seq, timestamp = feed.read_state_data()
    if not seq:
        print "Cannot continue. Can't read state data file."
        sys.exit(-1)

    print "Last replication sequence: %d on %s" % (seq, timestamp)
    feed.generate_entry(config.OUTPUT_DIR, seq, timestamp)

if len(sys.argv) == 2:
    # check to make sure we dont already have a last updated file
    seq, timestamp = feed.read_state_data()
    if seq:
        print "A %s file already exists. If you would like to start a new feed, remove this file first." % feed.LAST_UPDATED_FILE
        sys.exit(-1)

    seq = int(sys.argv[1])
    timestamp = feed.get_timestamp_from_replication_packet(seq)
    if not timestamp:
        print "Cannot fetch timestamp from replication packet"
        sys.exit(-1)

    print "Replication packet %s has timestamp %s" % (seq, timestamp)
#    if feed.get_timestamp_from_replication_packet(seq + 1):
#        print "You didn't specify the latest replication packet. Fuss on you."
#        sys.exit(-1)

    if not feed.save_state_data(seq, timestamp):
        print "Failed to save state data to %s" % feed.LAST_UPDATED_FILE
        print "Abort"
        sys.exit(-1)

    print "Saved the timestamp for the last replication packet. The\n"    + \
          "MusicBrainz rss feed should now be set up. Call this script\n" + \
          "once an hour after a new replication packet is available\n"    + \
          "to start dumping rss feed packets.\n"
