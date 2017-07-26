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

Then other tables with starting by 'rel' will be created that contains the information from musicbrainz links tables
"""

groups = {}
recordings = {}
works = {}
recording_data = {}
next_id=0

def get_next_group_id():
    global next_id
    # This method generates the ids of the recording_groups
    next_id += 1
    return next_id


def update_recordings(groups_to_merge, recordings_same_group):
    global recordings, groups, works
    prev_size = len(groups_to_merge)
    # Merge a list of recording_groups to a single recording_group
    for group in groups_to_merge:
        for rec in groups[group]:
            recordings_same_group.update([rec])
    for rec in recordings_same_group:
        if rec in recordings:
            groups_to_merge.update([recordings[rec]])

    if prev_size != len(groups_to_merge):
        update_recordings(groups_to_merge, recordings_same_group)
    else:
        next = get_next_group_id()
        for group in groups_to_merge:
            del groups[group]
            if group in works:
                works[next] = works[group]
                del works[group]

        for rec in recordings_same_group:
            recordings[rec] = next

        groups[next] = list(recordings_same_group)

def save_results(conn, cur):
    global recordings, groups, works, recording_data
    # Save the results in a new table for groups

    # create tables
    cur.execute("CREATE TABLE IF NOT EXISTS recording_group_recording(recording_id INTEGER, recording_group UUID, work_id INTEGER );")
    cur.execute("CREATE TABLE IF NOT EXISTS recording_group_artist(artist INTEGER, recording_group UUID);")
    cur.execute("CREATE TABLE IF NOT EXISTS recording_group(recording_group UUID, name VARCHAR);")

    new_groups = {}
    for group in groups.keys():
        group_uuid = uuid.uuid4()
        artists = set()
        insert_recs = []
        insert_recs_name = ()
        for rec in groups[group]:
            if group in works:
                insert_recs.append((rec, str(group_uuid), works[group]))
            else:
                insert_recs.append((rec, str(group_uuid), None))


            for artist in recording_data[rec]['artists']:
                artists.update([artist])

            # pick only one name for the recording_group
            insert_recs_name = (str(group_uuid), recording_data[rec]['name'])

        new_groups[str(group_uuid)] = groups[group]
        extras.execute_values (
            cur, "INSERT INTO recording_group_recording VALUES %s;", insert_recs, template=None, page_size=100
        )
        insert_recs_artists = []
        for artist in artists:
            insert_recs_artists.append((artist, str(group_uuid)))

        extras.execute_values (
            cur, "INSERT INTO recording_group_artist VALUES %s;", insert_recs_artists, template=None, page_size=100
        )

        cur.execute( "INSERT INTO recording_group VALUES (%s, %s);", insert_recs_name)

    conn.commit()

    groups = new_groups

    # Now generate release_group table linked to the recording_groups
    cur.execute("CREATE TABLE IF NOT EXISTS release_group_recording(release_group UUID, release_group_id INTEGER, recording UUID, recording_id INTEGER);")
    cur.execute("INSERT INTO release_group_recording (select rg.gid, rg.id, r.gid, r.id from musicbrainz.recording r, musicbrainz.track t, musicbrainz.medium m, musicbrainz.release re, musicbrainz.release_group rg WHERE r.id =t.recording AND t.medium = m.id AND m.release = re.id AND re.release_group = rg.id);")

    cur.execute("CREATE TABLE IF NOT EXISTS release_group_recording_group(release_group UUID, recording_group UUID);")
    cur.execute("INSERT INTO release_group_recording_group (select rlg.release_group, rgr.recording_group from release_group_recording rlg, recording_group_recording rgr WHERE rlg.recording_id = rgr.recording_id GROUP BY rlg.release_group, rgr.recording_group);")

    cur.execute("CREATE TABLE IF NOT EXISTS release_group_artist(release_group UUID, artist INTEGER);")
    cur.execute("INSERT INTO release_group_artist (select r.gid, acn.artist FROM musicbrainz.artist_credit ac, musicbrainz.artist_credit_name acn, musicbrainz.release_group r WHERE ac.id = acn.artist_credit AND r.artist_credit = ac.id);")

    conn.commit()

    ##--------##
    ##  LINKS ##
    ##--------##

    # create tables
    cur.execute("CREATE TABLE IF NOT EXISTS rel_artist_recording_group(artist_id INTEGER, recording_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_rg_pk PRIMARY KEY (artist_id, recording_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_artist_release_group(artist_id INTEGER, release_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_rlg_pk PRIMARY KEY (artist_id, release_group, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS rel_artist_place(artist_id INTEGER, place_id INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_p_pk PRIMARY KEY (artist_id, place_id, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS rel_recording_group_recording_group(recording_group UUID, recording_group2 UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_rc_rc_pk PRIMARY KEY (recording_group, recording_group2, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS rel_release_group_release_group(release_group UUID, release_group2 UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_rg_rg_pk PRIMARY KEY (release_group, release_group2, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS rel_artist_artist(artist INTEGER, artist2 INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_a_pk PRIMARY KEY (artist, artist2, link_type));")    
    cur.execute("CREATE TABLE IF NOT EXISTS rel_area_recording_group(area_id INTEGER, recording_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_ar_rg_pk PRIMARY KEY (area_id, recording_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_area_release_group(area_id INTEGER, release_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_ar_rlg_pk PRIMARY KEY (area_id, release_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_area_area(area_id INTEGER, area_id2 INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_ar_ar_pk PRIMARY KEY (area_id, area_id2, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_place_recording_group(place_id INTEGER, recording_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_p_rg_pk PRIMARY KEY (place_id, recording_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_place_release_group(place_id INTEGER, release_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_p_rlg_pk PRIMARY KEY (place_id, release_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_place_place(place_id INTEGER, place_id2 INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_p_p_pk PRIMARY KEY (place_id, place_id2, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_label_label(label_id INTEGER, label_id2 INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_l_l_pk PRIMARY KEY (label_id, label_id2, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_label_recording_group(label_id INTEGER, recording_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_l_l_pk PRIMARY KEY (label_id, recording_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_label_release_group(label_id INTEGER, release_group UUID, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_l_rlg_pk PRIMARY KEY (label_id, release_group, link_type));")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_artist_label(artist_id INTEGER, label_id INTEGER, link_type INTEGER, link_label VARCHAR, CONSTRAINT rel_a_l_pk PRIMARY KEY (artist_id, label_id, link_type));")

    cur.execute("INSERT INTO rel_artist_recording_group (select lar.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_artist_recording lar, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = rgr.recording_id)  ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO rel_artist_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_artist_work law, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.work_id)  ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO rel_artist_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_artist_release_group lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = rg.id)  ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO rel_artist_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_artist_release lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release r, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = r.id AND r.release_group = rg.id)  ON CONFLICT DO NOTHING;")


    cur.execute("INSERT INTO rel_recording_group_recording_group (select rgr.recording_group, rgr2.recording_group, l.link_type, lt.name from musicbrainz.l_recording_recording lrr, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr, recording_group_recording rgr2 WHERE lrr.link = l.id AND l.link_type = lt.id AND lrr.entity1 = rgr.recording_id AND lrr.entity0 = rgr2.recording_id AND rgr.recording_group <> rgr2.recording_group  )  ON CONFLICT DO NOTHING;")   
    
    cur.execute("INSERT INTO rel_recording_group_recording_group (select rgr.recording_group, rgr2.recording_group, l.link_type, lt.name from musicbrainz.l_work_work lrr, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr, recording_group_recording rgr2 WHERE lrr.link = l.id AND l.link_type = lt.id AND lrr.entity1 = rgr.work_id AND lrr.entity0 = rgr2.recording_id AND rgr.recording_group <> rgr2.recording_group  ) ON CONFLICT DO NOTHING;")  
    
    cur.execute("INSERT INTO rel_release_group_release_group (select rg.gid, rg2.gid, l.link_type, lt.name from musicbrainz.l_release_release lrr, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release r, musicbrainz.release r2, musicbrainz.release_group rg, musicbrainz.release_group rg2 WHERE lrr.link = l.id AND l.link_type = lt.id AND lrr.entity1 = r.id AND lrr.entity0 = r2.id AND r.release_group = rg.id AND  r2.release_group = rg2.id AND r.release_group <> r2.release_group  ) ON CONFLICT DO NOTHING;")  


    cur.execute("INSERT INTO rel_artist_artist (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_artist_artist laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id )  ON CONFLICT DO NOTHING;")    
    
    cur.execute("INSERT INTO rel_area_recording_group (select lar.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_area_recording lar, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = rgr.recording_id)  ON CONFLICT DO NOTHING;")   
    
    cur.execute("INSERT INTO rel_area_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_area_work law, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.work_id)  ON CONFLICT DO NOTHING;")  
    
    cur.execute("INSERT INTO rel_area_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_area_release lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = rg.id)  ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO rel_area_area (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_area_area laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id );") 
    
    cur.execute("INSERT INTO rel_place_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_place_recording law, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.recording_id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO rel_place_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_place_work law, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.work_id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO rel_place_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_place_release lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release r, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = r.id AND r.release_group = rg.id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO rel_artist_place (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_artist_place laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id ) ON CONFLICT DO NOTHING;") 

    cur.execute("INSERT INTO rel_place_place (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_place_place laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id ) ON CONFLICT DO NOTHING;")  
    
    cur.execute("INSERT INTO rel_label_label (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_label_label laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id ) ON CONFLICT DO NOTHING;")  
    
    cur.execute("INSERT INTO rel_label_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_label_recording law, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.recording_id) ON CONFLICT DO NOTHING;")
    cur.execute("INSERT INTO rel_label_recording_group (select law.entity0, rgr.recording_group, l.link_type, lt.name from musicbrainz.l_label_work law, musicbrainz.link l, musicbrainz.link_type lt, recording_group_recording rgr WHERE law.link = l.id AND l.link_type = lt.id AND law.entity1 = rgr.work_id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO rel_label_release_group (select lar.entity0, rg.gid, l.link_type, lt.name from musicbrainz.l_label_release lar, musicbrainz.link l, musicbrainz.link_type lt, musicbrainz.release r, musicbrainz.release_group rg WHERE lar.link = l.id AND l.link_type = lt.id AND lar.entity1 = r.id AND r.release_group = rg.id) ON CONFLICT DO NOTHING;")
    
    cur.execute("INSERT INTO rel_artist_label (select laa.entity0, laa.entity1, l.link_type, lt.name from musicbrainz.l_artist_label laa, musicbrainz.link l, musicbrainz.link_type lt WHERE laa.link = l.id AND l.link_type = lt.id ) ON CONFLICT DO NOTHING;") 
    
    conn.commit()

def main():
    global recordings, groups, works, recording_data

    conn = psycopg2.connect(dbname="musicbrainz", user="postgres", password="")

    # Get from the links table the relations between recording and work, grouped by work.
    # For each row we have to create a recording_group
    cur = conn.cursor()
    cur.execute("SELECT l.entity1, array_agg(l.entity0) as recs FROM musicbrainz.l_recording_work l GROUP BY l.entity1;")
    for work in cur.fetchall():
        new_group = get_next_group_id()
        groups[new_group] = []
        for rec in work[1]:
            recordings[rec] = new_group
            groups[new_group].append(rec)
            works[new_group] = work[0]

    # Group all the recordings of the same artist by name, then add all this as groups and merge whith the previous results
    cur.execute("SELECT acn.artist, lower(r.name), array_agg(r.id) FROM musicbrainz.artist_credit ac, musicbrainz.artist_credit_name acn, musicbrainz.recording r WHERE ac.id = acn.artist_credit AND r.artist_credit = ac.id GROUP BY acn.artist, lower(r.name)")
    for group in cur.fetchall():
        update_recordings(set(), set(group[2]))
        for rec in group[2]:
            if rec not in recording_data:
                recording_data[rec] = {'artists': [group[0]], 'name': group[1]}
            else:
                recording_data[rec]['artists'].append(group[0])

    cur.execute("select isrc, count(*), array_agg(recording) from musicbrainz.isrc group by isrc having count(isrc)>1")
    for group in cur.fetchall():
        update_recordings(set(), set(group[2]))


    save_results(conn, cur)
    cur.close()
    conn.close()


if __name__ == "__main__":

    main()
