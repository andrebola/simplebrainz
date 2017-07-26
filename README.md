# simplebrainz

Introduction
-----

The idea behind this project is to build a simplified version of musicbrainz schema for different purposes. The final goal is to build a music knowledge base with the musicbrainz information.

This problem has been already tacked in different projects, in this case we start with a different approach by creating some tables in the database that group some entities of musicbrainz. (See: https://wiki.musicbrainz.org/LinkedBrainz)

Steps
-----

To reproduce this task the initial step is to download the musicbrainz dump from here: http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/ and then create a database with the data following the project: https://github.com/lalinsky/mbslave 

Then you should run the script 'create_tables.py' which will create the new tables and fill with the data od musicbrainz. This script will take about 24 hours. At this point you will have a simplified version of the musicbrainz schema.

The next step is to create a dump in RDF of all the data, for that you could use our mappings to RDF or create your own, we provide the scripts we used to generate the mapping files (full_mapping_simplebrainz.n3)

To generate the dump you should first download the application from http://d2rq.org/, then start the service:


```
./d2r-server full_mapping_simplebrainz.n3
```

And then generate the dump, this process like take about a week or more if you don't limit the output:

```
./dump-rdf -f N-TRIPLE  -b http://localhost:2020/ /home/andres/projects/D2R-LinkedBrainz-Fork/musicbrainz_mapping.n3 > /home/andres/projects/D2R-LinkedBrainz-Fork/dump_simplebrainz.ttf
```
