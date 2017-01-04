hub = u"""{
    "title": "Hub",
    "type": "object",
    "properties": {
        "Enterprise Code": {
            "position": "0",
            "type": "string"
        },
        "Enterprise_Description": {
            "position": "1",
            "type": "string"
        },
        "Site_Name": {
            "position": "2",
            "type": "string"
            },
        "Site_Description": {
            "position": "3",
            "type": "string"
            },
        "Site_Type": {
            "position": "4",
            "type": "string"
        },
}"""

import json
import python_jsonschema_objects as pjs
from io import StringIO
import numpy

read_schema = StringIO(hub)
spec = json.load(read_schema)
# Uset the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
hub = ns.Hub


import random, string


def random_int(length):
    valid_letters = '1234567890'
    return ''.join((random.choice(valid_letters) for i in xrange(length)))


def random_word(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def random_pick(choices):
    return random.choice(choices)


def enrich_msg_lines(gen_line_with_attribute, zip_line):
    gen_line_with_attribute.Enterprise_Code= zip_line[0]
    gen_line_with_attribute.Enterprise_Description = zip_line[1]
    gen_line_with_attribute.Site_Name = zip_line[2]
    gen_line_with_attribute.Site_Description = zip_line[3]
    gen_line_with_attribute.Site_Type = zip_line[4]

import pickle

def serialize_msg_structure():
    global msg_structure
    with open('LOCATION-MASTER.ser', 'wb') as f:
        # order of msg_structure is changed as it is sorted in place
        for each_msg_line in msg_structure:
            pickle.dump(each_msg_line.as_dict(), f)

def deserialize_msg_structure():
    objects = []
    global msg_structure_reload;
    with open('LOCATION-MASTER.ser', 'rb') as openfile:
        while True:
            try:
                objects.append(pickle.load(openfile))
            except EOFError:
                break


import numpy
gen_line_with_attribute = hub()
loc_file = open ("Hub-1.0", "r")
loc_file_as_string = loc_file.read()
d = StringIO(unicode(loc_file_as_string))
data = numpy.loadtxt(d, dtype='str', delimiter='|', usecols=(0,1,2,3,4), unpack=True, skiprows=1)
#arr = numpy.genfromtxt(d, delimiter=('|'), autostrip=True)
data_zipped = zip(data[0], data[1], data[2], data[3])
for each_value in data_zipped:
    enrich_msg_lines(gen_line_with_attribute, data_zipped)


validate





