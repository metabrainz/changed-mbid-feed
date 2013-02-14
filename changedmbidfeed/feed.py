#!/usr/bin/env python

import sys
import os
import psycopg2
import json
import urllib2
import yaml
import tempfile
import tarfile
import shutil
import subprocess
import shutil
from config import PG_CONNECT, OUTPUT_DIR
from changed_ids import get_changed_ids
from log import log

VERSION =           "0.1"
PACKAGE =           "changed-mbid-feed"
LAST_UPDATED_FILE = os.path.join(OUTPUT_DIR, "last_updated")
DATA_DIR =          os.path.join(OUTPUT_DIR, "data")

def read_state_data():
    """Read the last replication sequence and date from a YAML file and return as a tuple"""

    try:
        f = open(LAST_UPDATED_FILE, "r")
    except IOError, e:
        return (None, None)

    try:
        ytext = f.read()
    except IOError, e:
        ytext = ""

    f.close();

    data = yaml.load(ytext)
    return (int(data['replication_sequence']), data['timestamp'])
            

def save_state_data(sequence, timestamp):
    """Write last replication sequence and data to a YAML file. return True/False"""

    try:
        os.makedirs(OUTPUT_DIR)
    except os.error, e:
        if e.errno != 17: # dir exists
            log("Error: cannot create data dir %s: %s\n" % (OUTPUT_DIR, str(e)))
            return False

    try:
        f = open(LAST_UPDATED_FILE, "w")
    except IOError, e:
        log("Warning: cannot write last date file %s: %s" % (LAST_UPDATED_FILE, e))
        return False

    data = { 'replication_sequence' : sequence, 'timestamp' : timestamp }
    try:
        f.write(yaml.dump(data) + "\n")
        f.close()
        return True
    except IOError, e:
        f.close();
        return False

def get_timestamp_from_replication_packet(sequence):

    packet = "ftp://ftp.musicbrainz.org/pub/musicbrainz/data/replication/replication-%d.tar.bz2" % sequence
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', '%s/%s' % (PACKAGE, VERSION))]

    log("downloading: %s" % packet)
    try:
        response = opener.open(packet)
    except urllib2.HTTPError, err:
        if err.code == 404:
            log("Replication packet %d not available." % sequence)
            return None
        log(err.code)
        return None
    except urllib2.URLError, err:
        if err.reason.find("550"):
            log("Replication packet %d not available." % sequence)
            return None
        log(err.reason)
        return None

    tmp = tempfile.TemporaryFile()
    shutil.copyfileobj(response, tmp)
    tmp.seek(0)

    try:
        tar_file = tarfile.open(mode="r:bz2", fileobj=tmp)
    except tarfile.ReadError:
        log("Cannot read tar file. Is the packet corrupt?")
        return None

    for f in tar_file:
        if f.name == 'TIMESTAMP':
            timestamp = tar_file.extractfile(f).read()

    return timestamp.strip()

def save_data(data_dir, sequence, timestamp, data):
    '''Save the data file. If its not written for whatever reason, don't return. die.'''

    try:
        os.makedirs(DATA_DIR)
    except os.error, e:
        if e.errno != 17: # dir exists
            sys.stderr.write("Error: cannot create data dir %s: %s\n" % (OUTPUT_DIR, str(e)))
            sys.exit(-1)

    filename = os.path.join(DATA_DIR, "changed-ids-%d.json" % sequence)
    try:
        f = open(filename, "w")
    except os.error, e:
        sys.stderr.write("Error: cannot create data file %s: %s\n" % (filename, str(e)))
        sys.exit(-1)

    try:
        f.write(data + "\n");
    except os.error, e:
        sys.stderr.write("Error: cannot write to file: %s\n" % str(e))
        f.close();
        os.unlink(filename)
        sys.exit(-1)

    f.close()

    try:
        subprocess.check_call(["gzip", filename])
    except subprocess.CalledProcessError, e:
        sys.stderr.write("Error: cannot compress output file: %s\n" % str(e))
        f.close();
        os.unlink(filename)
        sys.exit(-1)

def save_copying(data_dir):
    '''Save the COPYING file into the directory with all the data files'''

    copying = os.path.join(data_dir, "data/COPYING")
    if not os.path.exists(copying):
        try:
            shutil.copy(os.path.join(os.path.dirname(__file__), "../extra/COPYING-PublicDomain"), copying)
        except shutil.Error:
            sys.stderr.write("cannot write COPYING file: %s\n" % str(e))
            sys.exit(-1)
            

def save_latest_file(data_dir, seq):
    '''Create the LATEST file that gives the name of the latest and greatest file'''
    latest = os.path.join(data_dir, "data/LATEST")
    try:
        f = open(latest, "w")
        f.write("%s\n" % seq)
        f.close()
    except IOError, e:
        sys.stderr.write("cannot write LATEST file: %s\n" % str(e))
        sys.exit(-1)

def get_current_replication_info():
    try:
        conn = psycopg2.connect(PG_CONNECT)
    except psycopg2.OperationalError as err:
        log("Cannot connect to database: %s" % err)
        sys.exit(-1)

    cur = conn.cursor()
    cur.execute("select current_replication_sequence, last_replication_date from replication_control");
    row = cur.fetchone()

    return row[0], str(row[1])

def generate_entry(data_dir, last_sequence, last_timestamp):

    current_sequence, current_timestamp = get_current_replication_info()

    log("   last: %d at %s" % (last_sequence, last_timestamp))
    log("current: %s at %s" % (current_sequence, current_timestamp))
    if last_sequence == current_sequence:
        log("Replication sequence unchanged, no new data to process.")
        return None

    data = get_changed_ids(last_timestamp, current_timestamp)
    data = json.dumps({ 'data' : data }) #, sort_keys=True, indent=4)
    save_data(data_dir, current_sequence, current_timestamp, data)
    save_state_data(current_sequence, current_timestamp)
    save_copying(data_dir)
    save_latest_file(data_dir, current_sequence)
    log("changed mbid processing complete")
