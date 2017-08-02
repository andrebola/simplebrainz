
import psycopg2

"""
Given a basic mapping file with this script we add all the missing mappings that take the information from the links tables
"""
MAPPING_FOLDER = ""

def main():
    conn = psycopg2.connect(dbname="musicbrainz", user="postgres", password="")

    cur = conn.cursor()
    
    tables = [
            ("rel_artist_recording_group", "Artist", "RecordingGroup", "musicbrainz.artist.id", "artist_id", "recording_group", "simplebrainz.recording_group.recording_group"),
            ("rel_artist_release_group", "Artist", "ReleaseGroup", "musicbrainz.artist.id", "artist_id", "release_group", "musicbrainz.release_group.gid"),
            ("rel_artist_place", "Artist", "Place", "musicbrainz.artist.id", "artist_id", "place_id", "musicbrainz.place.id"),
            ("rel_recording_group_recording_group", "RecordingGroup", "RecordingGroup", "simplebrainz.recording_group.recording_group", "recording_group", "recording_group2", "simplebrainz.recording_group.recording_group"),
            ("rel_artist_artist","Artist", "Artist", "musicbrainz.artist.id", "artist", "artist2", "musicbrainz.artist.id"),
            ("rel_release_group_release_group","ReleaseGroup", "ReleaseGroup", "musicbrainz.release_group.gid", "release_group", "release_group2", "musicbrainz.release_group.gid"),
            ("rel_area_recording_group", "Area", "RecordingGroup", "musicbrainz.area.id", "area_id", "recording_group", "simplebrainz.recording_group.recording_group"),
            ("rel_area_release_group","Area", "ReleaseGroup", "musicbrainz.area.id", "area_id", "release_group", "musicbrainz.release_group.gid"),
            ("rel_area_area", "Area", "Area", "musicbrainz.area.id", "area_id", "area_id2", "musicbrainz.area.id"),
            ("rel_place_recording_group", "Place", "RecordingGroup", "musicbrainz.place.id", "place_id", "recording_group",  "simplebrainz.recording_group.recording_group"),
            ("rel_place_release_group", "Place", "ReleaseGroup", "musicbrainz.place.id", "place_id", "release_group", "musicbrainz.release_group.gid"),
            ("rel_place_place", "Place", "Place", "musicbrainz.place.id", "place_id","place_id2",  "musicbrainz.place.id"),
            ("rel_label_label", "Label", "Label", "musicbrainz.label.id", "label_id", "label_id2", "musicbrainz.label.id"),
            ("rel_label_recording_group","Label", "RecordingGroup", "musicbrainz.label.id", "label_id", "recording_group",  "simplebrainz.recording_group.recording_group"),
            ("rel_label_release_group", "Label", "ReleaseGroup", "musicbrainz.label.id", "label_id", "release_group", "musicbrainz.release_group.gid"),
            ("rel_artist_label", "Artist", "Label", "musicbrainz.artist.id", "artist_id", "label_id", "musicbrainz.label.id")
             ]
   
    alias_counter = 0
    output = []
    for table, c1, c2, t1, co1, co2, t2 in tables:
        cur.execute("select link_label, link_type, reverse_link from simplebrainz.%s group by link_label, link_type, reverse_link ;" % (table))
        for links in cur.fetchall():
            short_name = links[0].replace(' ', '_').replace('/', '_')
            reverse_short_name = re.sub('\{.*?\}','', links[2])
            reverse_short_name = reverse_short_name.replace(' ', '_').replace('/', '_')
            if co1+"2" == co2:
                same_table_name = ".".join(t2.split('.')[0:-1])
                same_table_field = t2.split('.')[-1]
                output.append("""map:%s_%s a d2rq:PropertyBridge ;
                    d2rq:belongsToClassMap map:%s ;
                    d2rq:property sb:%s ;
                    d2rq:refersToClassMap map:%s ;
                    d2rq:condition "simplebrainz.%s.link_type =%i " ;
                    d2rq:alias "%s as rg%d";
                    d2rq:join "%s=> simplebrainz.%s.%s" ;
                    d2rq:join "simplebrainz.%s.%s => rg%d.%s" .\n\n""" % (table, short_name, c1, short_name, c2, table, links[1], same_table_name, alias_counter, t1, table, co1, table, co2, alias_counter, same_table_field))
                alias_counter +=1
                output.append("""map:inverse_%s_%s a d2rq:PropertyBridge ;
                    d2rq:belongsToClassMap map:%s ;
                    d2rq:property sb:%s ;
                    d2rq:refersToClassMap map:%s ;
                    d2rq:condition "simplebrainz.%s.link_type =%i " ;
                    d2rq:alias "%s as rg%d";
                    d2rq:join "%s=> simplebrainz.%s.%s" ;
                    d2rq:join simplebrainz."%s.%s => rg%d.%s" .\n\n""" % (table, reverse_short_name, c2, reverse_short_name, c1, table, links[1], same_table_name, alias_counter, t1, table, co2, table, co1, alias_counter, same_table_field))
                alias_counter +=1

            else:
                output.append("""map:%s_%s a d2rq:PropertyBridge ;
                    d2rq:belongsToClassMap map:%s ;
                    d2rq:property sb:%s ;
                    d2rq:refersToClassMap map:%s ;
                    d2rq:condition "simplebrainz.%s.link_type =%i " ;
                    d2rq:join "%s=> simplebrainz.%s.%s" ;
                    d2rq:join "simplebrainz.%s.%s => %s" .\n\n""" % (table, short_name, c1, short_name, c2, table, links[1], t1, table, co1, table, co2, t2))
                output.append("""map:%s_%s a d2rq:PropertyBridge ;
                    d2rq:belongsToClassMap map:%s ;
                    d2rq:property sb:%s ;
                    d2rq:refersToClassMap map:%s ;
                    d2rq:condition "simplebrainz.%s.link_type =%i " ;
                    d2rq:join "%s=> simplebrainz.%s.%s" ;
                    d2rq:join "simplebrainz.%s.%s => %s" .\n\n""" % (table, reverse_short_name, c2, reverse_short_name, c1, table, links[1], t2, table, co2, table, co1, t1))

    cur.close()
    conn.close()
    
    first_part_mapping = open(MAPPING_FOLDER+"musicbrainz_mapping_links.n3", "r")
    text_file = open(MAPPING_FOLDER+"full_mapping_simplebrainz.n3", "w")
    text_file.write(first_part_mapping.read() + '\n'.join(output))
    text_file.close()


if __name__ == "__main__":
    
    """
        ./dump-rdf -f N-TRIPLE  -b http://localhost:2020/ /home/andres/projects/D2R-LinkedBrainz-Fork/musicbrainz_mapping.n3 > /home/andres/projects/D2R-LinkedBrainz-Fork/dump_simplebrainz.ttf
    """
    main()
