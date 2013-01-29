changed-mbid-feed
=================

This project creates a data-feed of changed MBID in the last hour. This parallels the live-data-feed
that MusicBrainz already has, but presents the data in a much more user-friendly fashion. There will be
two data streams in this project:

  1. changed-ids stream: a JSON file of all of the MBIDs that changed in the last hour. See the
     directory examples for an example file. 
  2. changed-ids-with-data stream: a tar file with the JSON file from step #1 a subdirectory
     that contains files named after the MBIDs that changed, with the .json extension. These
     files will contain the JSON data for each entity, as returned by the JSON enabled MB
     web service. This part isn't implemented yet -- we'll first debug the ID generation and then
     we can see about adding that actual data into the stream.

All of these files will be available on the FTP server before too long. The file system structure for 
this will look like this:

    <output dir>/last_updated
    <output dir>/data/changed-ids-61476.json.gz
    <output dir>/data/changed-ids-with-data-61476.json.gz
    <output dir>/data/changed-ids-61477.json.gz
    <output dir>/data/changed-ids-with-data-61477.json.gz

Install instructions
====================

Besides a working python installl and a copy of the replicated MusicBrainz database you'll need
the following python modules:

psycopg2
yaml
