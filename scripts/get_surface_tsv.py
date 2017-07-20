"""
Given a ttf file with this script we generate a tsv with the same data
"""
source_ttf = "/path/to/data.ttf"

def load_types():
    f = open(source_ttf)
    fw = open('/---destination---/surface_forms.tsv', 'w')
    uri_to_types = dict()
    for line in f:
        data = line.strip().split(" ")

        if data[1].endswith("label>"):
            uri_token = data[0]
            relation = data[1]
            type_token = ' '.join(data[2:])
            token = type_token.split("\"^^")
            fw.write(uri_token[1:-1]+"\t"+token[0][1:]+"\n")
    f.close()

if __name__ == "__main__":

    load_types()
