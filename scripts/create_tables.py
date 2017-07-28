import json
import psycopg2
import uuid
from psycopg2 import extras


"""

Script for generating a simplified version of the musicbrainz schema, we add the concept of recording_group and related with release_group and artist:

After running the script the following tables will be created:

    recording_group
    recording_group_artist
    recording_group_recording

    release_group_recording
    release_group_recording_group
    release_group_artist

"""

groups = {}
recordings = {}
works = {}
recording_data = {}
work_data = {}

def save_results(conn, cur):
    global recordings, groups, works, recording_data
    # Save the results in a new table for groups
    
    # create tables
    cur.execute("CREATE SCHEMA IF NOT EXISTS simplebrainz;")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.recording_group_recording(recording_id INTEGER, recording_group UUID, work_id INTEGER );")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.recording_group_artist(artist INTEGER, recording_group UUID);")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.recording_group(recording_group UUID, name VARCHAR);")
    
    insert_works = []
    for work in works.keys():
        insert_works.append((work, work_data[work]))
        if len(insert_works) == 100000:
            extras.execute_values (
                cur, "INSERT INTO simplebrainz.recording_group VALUES %s;", insert_works, template=None, page_size=100
            )
            insert_works = []
    extras.execute_values (
        cur, "INSERT INTO simplebrainz.recording_group VALUES %s;", insert_works, template=None, page_size=100
    )
    for rec in recordings.keys():
        insert_recs = []
        insert_recs_artists = []
        for work in recordings[rec]:
            insert_recs.append((rec, work, works[work]))
            if rec in recording_data:
                for artist in recording_data[rec]['artists']:
                    insert_recs_artists.append((artist, work))
        if len(insert_recs) == 100000:
            extras.execute_values (
                cur, "INSERT INTO simplebrainz.recording_group_recording VALUES %s;", insert_recs, template=None, page_size=100
            )
            insert_recs = []
        if len(insert_recs_artists) == 100000:
            extras.execute_values (
                cur, "INSERT INTO simplebrainz.recording_group_artist VALUES %s;", insert_recs_artists, template=None, page_size=100
            )
            insert_recs_artists = []
    extras.execute_values (
        cur, "INSERT INTO simplebrainz.recording_group_recording VALUES %s;", insert_recs, template=None, page_size=100
    )
    extras.execute_values (
        cur, "INSERT INTO simplebrainz.recording_group_artist VALUES %s;", insert_recs_artists, template=None, page_size=100
    )

    conn.commit()
    
    # Now generate release_group table linked to the recording_groups
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.release_group_recording(release_group UUID, release_group_id INTEGER, recording UUID, recording_id INTEGER);")
    cur.execute("INSERT INTO simplebrainz.release_group_recording (select rg.gid, rg.id, r.gid, r.id from musicbrainz.recording r, musicbrainz.track t, musicbrainz.medium m, musicbrainz.release re, musicbrainz.release_group rg WHERE r.id =t.recording AND t.medium = m.id AND m.release = re.id AND re.release_group = rg.id);")

    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.release_group_recording_group(release_group UUID, recording_group UUID);")
    cur.execute("INSERT INTO simplebrainz.release_group_recording_group (select rlg.release_group, rgr.recording_group from simplebrainz.release_group_recording rlg, simplebrainz.recording_group_recording rgr WHERE rlg.recording_id = rgr.recording_id GROUP BY rlg.release_group, rgr.recording_group);")
    
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.release_group_artist(release_group UUID, artist INTEGER);")
    cur.execute("INSERT INTO simplebrainz.release_group_artist (select r.gid, acn.artist FROM musicbrainz.artist_credit ac, musicbrainz.artist_credit_name acn, musicbrainz.release_group r WHERE ac.id = acn.artist_credit AND r.artist_credit = ac.id);")
    
    conn.commit()
        
    ##--------##
    ##  LINKS ##
    ##--------##
    
    # create tables
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_artist_recording_group(artist_id INTEGER, recording_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_rg_pk PRIMARY KEY (artist_id, recording_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_artist_release_group(artist_id INTEGER, release_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_rlg_pk PRIMARY KEY (artist_id, release_group, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_artist_place(artist_id INTEGER, place_id INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_p_pk PRIMARY KEY (artist_id, place_id, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_recording_group_recording_group(recording_group UUID, recording_group2 UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_rc_rc_pk PRIMARY KEY (recording_group, recording_group2, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_release_group_release_group(release_group UUID, release_group2 UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_rg_rg_pk PRIMARY KEY (release_group, release_group2, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_artist_artist(artist INTEGER, artist2 INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_a_pk PRIMARY KEY (artist, artist2, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_area_recording_group(area_id INTEGER, recording_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_ar_rg_pk PRIMARY KEY (area_id, recording_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_area_release_group(area_id INTEGER, release_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_ar_rlg_pk PRIMARY KEY (area_id, release_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_area_area(area_id INTEGER, area_id2 INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_ar_ar_pk PRIMARY KEY (area_id, area_id2, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_place_recording_group(place_id INTEGER, recording_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_p_rg_pk PRIMARY KEY (place_id, recording_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_place_release_group(place_id INTEGER, release_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_p_rlg_pk PRIMARY KEY (place_id, release_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_place_place(place_id INTEGER, place_id2 INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_p_p_pk PRIMARY KEY (place_id, place_id2, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_label_label(label_id INTEGER, label_id2 INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_l_l_pk PRIMARY KEY (label_id, label_id2, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_label_recording_group(label_id INTEGER, recording_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_l_r_pk PRIMARY KEY (label_id, recording_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_label_release_group(label_id INTEGER, release_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_l_rlg_pk PRIMARY KEY (label_id, release_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS simplebrainz.rel_artist_label(artist_id INTEGER, label_id INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_l_pk PRIMARY KEY (artist_id, label_id, link_type));")

    cur.execute("INSERT INTO simplebrainz.rel_artist_recording_group (select lar.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_artist_recording lar, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = rgr.recording_id)  ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO simplebrainz.rel_artist_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_artist_work law, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.work_id)  ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO simplebrainz.rel_artist_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_artist_release_group lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = rg.id)  ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO simplebrainz.rel_artist_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_artist_release lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release r, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = r.id AND r.release_group = rg.id)  ON CONFLICT DO NOTHING;")


    cur.execute("INSERT INTO simplebrainz.rel_recording_group_recording_group (select rgr.recording_group, rgr2.recording_group, l.link_type, lt.name from musicbrainz.l_recording_recording lrr, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr, simplebrainz.recording_group_recording rgr2 WHERE lrr.link = l.id AND l.link_type = lt.id AND lrr.entity1 = rgr.recording_id AND lrr.entity0 = rgr2.recording_id AND rgr.recording_group <> rgr2.recording_group  )  ON CONFLICT DO NOTHING;")   
    
    cur.execute("INSERT INTO simplebrainz.rel_recording_group_recording_group (select rgr.recording_group, rgr2.recording_group, l.link_type, lt.name from musicbrainz.l_work_work lrr, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr, simplebrainz.recording_group_recording rgr2 WHERE lrr.link = l.id AND l.link_type = lt.id AND lrr.entity1 = rgr.work_id AND lrr.entity0 = rgr2.recording_id AND rgr.recording_group <> rgr2.recording_group  ) ON CONFLICT DO NOTHING;")  
    
    cur.execute("INSERT INTO simplebrainz.rel_release_group_release_group (select rg.gid, rg2.gid, l.link_type, lt.name from musicbrainz.l_release_release lrr, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release r, musicbrainz.release r2, musicbrainz.release_group rg, musicbrainz.release_group rg2 WHERE lrr.link = l.id AND l.link_type = lt.id AND lrr.entity1 = r.id AND lrr.entity0 = r2.id AND r.release_group = rg.id AND  r2.release_group = rg2.id AND r.release_group <> r2.release_group  ) ON CONFLICT DO NOTHING;")  


    cur.execute("INSERT INTO simplebrainz.rel_artist_artist (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_artist_artist laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id )  ON CONFLICT DO NOTHING;")    
    
    cur.execute("INSERT INTO simplebrainz.rel_area_recording_group (select lar.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_area_recording lar, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = rgr.recording_id)  ON CONFLICT DO NOTHING;")   
    
    cur.execute("INSERT INTO simplebrainz.rel_area_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_area_work law, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.work_id)  ON CONFLICT DO NOTHING;")  
    
    cur.execute("INSERT INTO simplebrainz.rel_area_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_area_release lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = rg.id)  ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO simplebrainz.rel_area_area (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_area_area laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id );") 
    
    cur.execute("INSERT INTO simplebrainz.rel_place_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_place_recording law, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.recording_id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO simplebrainz.rel_place_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_place_work law, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.work_id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO simplebrainz.rel_place_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_place_release lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release r, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = r.id AND r.release_group = rg.id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO simplebrainz.rel_artist_place (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_artist_place laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id ) ON CONFLICT DO NOTHING;") 

    cur.execute("INSERT INTO simplebrainz.rel_place_place (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_place_place laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id ) ON CONFLICT DO NOTHING;")  
    
    cur.execute("INSERT INTO simplebrainz.rel_label_label (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_label_label laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id ) ON CONFLICT DO NOTHING;")  
    
    cur.execute("INSERT INTO simplebrainz.rel_label_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_label_recording law, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.recording_id) ON CONFLICT DO NOTHING;")
    cur.execute("INSERT INTO simplebrainz.rel_label_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_label_work law, musicbrainz.link l, musicbrainz.link_type lt, simplebrainz.recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.work_id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO simplebrainz.rel_label_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_label_release lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release r, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = r.id AND r.release_group = rg.id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO simplebrainz.rel_artist_label (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_artist_label laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id ) ON CONFLICT DO NOTHING;") 
    
    conn.commit()

def main():
    global recordings, groups, works, recording_data

    conn = psycopg2.connect(dbname="musicbrainz", user="postgres", password="")

    # Get from the links table the relations between recording and work, grouped by work.
    # For each row we have to create a recording_group
    cur = conn.cursor()
    cur.execute("SELECT w.gid, l.entity1, w.name , array_agg(l.entity0) as recs FROM musicbrainz.l_recording_work l, musicbrainz.work w WHERE w.id = l.entity1 GROUP BY w.gid, l.entity1, w.name")
    for work in cur.fetchall():
        for rec in work[3]:
            recordings.setdefault(rec,[]).append(work[0])
            works[work[0]] = work[1]
            work_data[work[0]] = work[2]

    # Group all the recordings of the same artist by name, then add all this as groups and merge whith the previous results
    cur.execute("SELECT acn.artist, lower(r.name), array_agg(r.id) FROM musicbrainz.artist_credit ac, musicbrainz.artist_credit_name acn, musicbrainz.recording r WHERE ac.id = acn.artist_credit AND r.artist_credit = ac.id GROUP BY acn.artist, lower(r.name);")
    for group in cur.fetchall():
        work = set()
        for rec in group[2]:
            try:
                work.update(recordings[rec])
            except:
                pass
            if rec not in recording_data:
                recording_data[rec] = {'artists': [group[0]], 'name': group[1]}
            else:
                recording_data[rec]['artists'].append(group[0])
        new_work = None
        if len(work) == 1:
            new_work = work.pop()
        elif len(work) == 0:
            group_uuid = uuid.uuid4()
            new_work = str(group_uuid)
        if new_work:
            for rec in group[2]:
                recordings[rec] = [new_work]
                works[new_work] = None
                work_data[new_work] = group[1]



    save_results(conn, cur)
    cur.close()
    conn.close()
    
    
if __name__ == "__main__":
    
    main()
