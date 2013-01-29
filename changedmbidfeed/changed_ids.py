#!/usr/bin/env python

import sys
import psycopg2;
from changedmbidfeed import queries
from changedmbidfeed.config import PG_CONNECT
from changedmbidfeed.log import log

def get_ids_for_entity(conn, queries, start, end):

    arguments = []
    ids = []

    for q in queries: arguments.extend((start, end))

    query = "SELECT DISTINCT(gid) FROM (\n";
    query += '\n  UNION\n'.join(queries)
    query += '\n) as gid'
    query = query.replace("%schema%", "musicbrainz")

    cur = conn.cursor()
    cur.execute(query, arguments)
    rows = cur.fetchall()
    for row in rows: ids.append(row[0])
    return ids

def get_changed_ids(start, end):
    try:
        conn = psycopg2.connect(PG_CONNECT)
    except psycopg2.OperationalError as err:
        log("Cannot connect to database: %s" % err)
        sys.exit(-1)

    data = {}
    entities = ('artist', 'label', 'recording', 'release_group', 'release', 'work')
    for entity in entities:
        data[entity] = get_ids_for_entity(conn, queries.queries[entity], start, end)
    return data
