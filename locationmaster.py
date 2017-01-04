import json
import python_jsonschema_objects as pjs
from io import StringIO

location_master = u"""{
    "title": "LOCATION MASTER",
    "type": "object",
    "properties": {
        "PLANT_ID": {
            "position": "0",
            "type": "string"
        },
        "PLANT_DESC": {
            "position": "1",
            "type": "string"
        },
        "FACTORY_CALENDAR_ID": {
            "description": "calendars",
            "position": "2",
            "type": "string"
            },
        "VENDOR_ID": {
            "description": "calendars",
            "position": "2",
            "type": "string"
            },
        "STREET": {
            "position": "3",
            "type": "string"
        },
        "CITY": {
            "position": "3",
            "type": "string"
        },
        "REGION_CODE": {
            "position": "4",
            "type": "string"
        },
        "COUNTRY_CODE": {
            "position": "5",
            "type": "string"
        },
        "ZIPCODE": {
            "position": "6",
            "type": "string"
        },
        "BLKD_STOCK_RLV_FLAG": {
            "position": "7",
            "type": "string"
        }
    },
    "required": ["PLANT_ID"]
}"""

position_of_header = ["PLANT_ID", "PLANT_DESC", "FACTORY_CALENDAR_ID", "VENDOR_ID", "STREET", "CITY", "REGION_CODE",
                "COUNTRY_CODE", "ZIPCODE", "BLKD_STOCK_RLV_FLAG"]

datatype_of_header = [("PLANT_ID", '>S1'), ("PLANT_DESC", '>S1'), ("FACTORY_CALENDAR_ID", '>S1'), ("VENDOR_ID", '>S1'), ("STREET", '>S1'), ("CITY", '>S1'), ("REGION_CODE", '>S1'),
                      ("COUNTRY_CODE", '>S1'), ("ZIPCODE", '>S1'), ("BLKD_STOCK_RLV_FLAG", '>S1')]


position_of_header_subset = ["PLANT_ID", "PLANT_DESC", "FACTORY_CALENDAR_ID", "VENDOR_ID", "STREET", "CITY", "REGION_CODE",
                  "COUNTRY_CODE", "ZIPCODE"]

read_schema = StringIO(location_master)
spec = json.load(read_schema)
# Uset the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
LocMaster = ns.LocationMaster
msg_structure = []
msg_structure_reload = []
import csv
import datetime
from itertools import ifilter
import random, string
from robot.api import logger
import pickle


def random_int(length):
    valid_letters = '1234567890'
    return ''.join((random.choice(valid_letters) for i in xrange(length)))


def random_word(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def random_pick(choices):
    return random.choice(choices)

#create message lines with these attributes
def enrich_msg_lines(gen_line_with_attribute):
    gen_line_with_attribute.PLANT_ID = random_pick([random_int(4), '8888'])
    gen_line_with_attribute.PLANT_DESC = gen_line_with_attribute.PLANT_ID + random_word(25)
    gen_line_with_attribute.FACTORY_CALENDAR_ID = random_word(2)
    gen_line_with_attribute.VENDOR_ID = random_word(10)
    gen_line_with_attribute.STREET = random_pick([random_word(12) + ' ' + random_word(10), 'St. Michael street'])
    gen_line_with_attribute.CITY = random_word(20)
    gen_line_with_attribute.REGION_CODE = random_word(20)
    gen_line_with_attribute.COUNTRY_CODE = random_int(3)
    gen_line_with_attribute.ZIPCODE = random_int(10)
    gen_line_with_attribute.BLKD_STOCK_RLV_FLAG = 'X'
    logger.console("Generate line is {0}".format(LocMaster))
    return gen_line_with_attribute


def msgline_to_list(msg_line):
    global msg_structure
    logger.console("Adding msg line to msg_structure")
    msg_structure.append(msg_line)



def number_of_lines(lines_to_generate=1):
    for each_line in range(lines_to_generate):
        gen_line_with_attribute = LocMaster()
        gen_line_with_attribute = enrich_msg_lines(gen_line_with_attribute)
        msgline_to_list(gen_line_with_attribute)

#messageio
#save
# generating different kinds of txt files (sorted, unsorted, filtered, rearragenged column, droppedcolumn, so on)
# repeating code below just to make it obvious snippet
def create_txt_file(msg_structure, file_name='LOCATION-MASTER.TXT'):

    if 'FILTER' in file_name:
        with open(file_name, 'wb') as csv_file:
            wr = csv.writer(csv_file, delimiter="|")
            #wr.writerow(msg_structure[0].keys())
            #msg_structure.sort(key=lambda x: (x.PLANT_ID, x.STREET), reverse=False)
            for msg_line in msg_structure:
                wr.writerow(msg_line.values())
            return True


    with open('LOCATION-MASTER.TXT-UNORDERED', 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter="|")
        wr.writerow(msg_structure[0].keys())
        msg_structure.sort(key=lambda x: (x.PLANT_ID, x.STREET), reverse=False)
        for msg_line in msg_structure:
            wr.writerow(msg_line.values())

    with open('LOCATION-MASTER.TXT-UNORDERED', 'rb') as input_file:
        with open(file_name, 'wb') as output_file:
            read_csv = csv.DictReader(input_file, delimiter='|')
            # save ignoring certain columns by using list: position_of_header or position_of_header_subset

            write_csv = csv.DictWriter(output_file, position_of_header, delimiter='|', extrasaction='ignore')
            write_csv.writeheader()
            for read_row in read_csv:
                write_csv.writerow(read_row)


    # create a test file and then later compare the outputs
    with open('LOCATION-MASTER.TXT-SORTED', 'wb') as sorted_file:
        wr = csv.writer(sorted_file, delimiter="|")
        wr.writerow(msg_structure[0].keys())
        msg_structure.sort(key=lambda x: (x.STREET, x.PLANT_ID), reverse=False)
        for msg_line in msg_structure:
            wr.writerow(msg_line.values())

#messageIO
#serialize
def serialize_msg_structure():
    with open('LOCATION-MASTER.ser', 'wb') as f:
        # order of msg_structure is changed as it is sorted in place
        for each_msg_line in msg_structure:
            pickle.dump(each_msg_line.as_dict(), f)

#messageIO
#load from file
def deserialize_msg_structure():
    objects = []
    global msg_structure_reload;
    with open('LOCATION-MASTER.ser', 'rb') as openfile:
        while True:
            try:
                objects.append(pickle.load(openfile))
            except EOFError:
                break

    for each_obj in objects:
        msg_line_as_json = json.dumps(each_obj)
        create_line_on_deserialization = LocMaster.from_json(msg_line_as_json)
        msg_structure_reload.append(create_line_on_deserialization)

#simple sort and compare
#Compare message_structures
def compare_message_before_after():
    msg_structure.sort(key=lambda x: (x.PLANT_ID, x.STREET), reverse=False)
    msg_structure_reload.sort(key=lambda x: (x.PLANT_ID, x.STREET), reverse=False)
    for i in range(len(msg_structure)) :
        if msg_structure[i].PLANT_ID == msg_structure_reload[i].PLANT_ID and \
                        msg_structure[i].VENDOR_ID == msg_structure_reload[i].VENDOR_ID:
            logger.console("Success")
        else:
            logger.console("Failed")

#filters and chain of filters
#each filters return true or false: add any other logic here
def f1(msg_line): return (msg_line.PLANT_ID == '8888')
def f2(msg_line): return (msg_line.STREET == 'St. Michael street')

def filter_using_non_lambda_way(filters, msg_structure):
    for f in filters:
        msg_structure = filter(f, msg_structure)
        if not msg_structure:
            return msg_structure
    return msg_structure

# all or any can be called in the filter below
def nFilter_one_liner(filters, msg_structure):
    return (t for t in msg_structure if any(f(t) for f in filters))

# all or any can be called in the filter below
def nFilter_using_lambda(filters, msg_structure):
    return ifilter(lambda t: all(f(t) for f in filters), msg_structure)


import numpy

loc_file = open ("LOCATION-MASTER.TXT", "r")
loc_file_as_string = loc_file.read()
d = StringIO(unicode(loc_file_as_string))
data = numpy.loadtxt(d, dtype='str', delimiter='|', usecols=(0,1,2,3,4,5,6,7,8,9), unpack=True, skiprows=1)
#arr = numpy.genfromtxt(d, delimiter=('|'), autostrip=True)

data_zipped = zip(data[0], data[1], data[2], data[3])

print data


hub = u"""{
    "title": "Hub",
    "type": "object",
    "properties": {
        "Enterprise_Code": {
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
        }
    }
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

hub_message_structure = []

def enrich_msg_lines_hub(gen_line_with_attribute, zip_line):
    gen_line_with_attribute.Enterprise_Code= str(zip_line[0])
    gen_line_with_attribute.Enterprise_Description = str(zip_line[1])
    gen_line_with_attribute.Site_Name = str(zip_line[2])
    gen_line_with_attribute.Site_Description = str(zip_line[3])
    gen_line_with_attribute.Site_Type = str(zip_line[4])
    hub_message_structure.append(gen_line_with_attribute)
    return hub_message_structure


import numpy

gen_line_with_attribute_hub = hub()
loc_file = open ("Hub-1.0", "r")
loc_file_as_string = loc_file.read()
d = StringIO(unicode(loc_file_as_string))
data = numpy.loadtxt(d, dtype='str', delimiter='\t', usecols=(0,1,2,3,4), unpack=True, skiprows=1)
#arr = numpy.genfromtxt(d, delimiter=('|'), autostrip=True)
data_zipped = zip(data[0], data[1], data[2], data[3], data[4])
for each_value in data_zipped:
    enrich_msg_lines_hub(gen_line_with_attribute_hub, each_value)

def f3(msg_line, i=1): return (msg_line.PLANT_ID == hub_message_structure[i].Site_Name)
def f4(msg_line, i=1): return (msg_line.STREET == hub_message_structure[i].Site_Description)


def filter_using_non_lambda_way2(filters, msg_structure):
    for f in filters:
        msg_structure = filter(f, msg_structure)
        if not msg_structure:
            return msg_structure
    return msg_structure

def validate_input_output():
    deserialize_msg_structure()
    msg_structure.sort(key=lambda x: (x.PLANT_ID, x.PLANT_DESC), reverse=False)
    my_filtered_msg_structure = filter_using_non_lambda_way2([f3, f4], msg_structure)
    return len(my_filtered_msg_structure)>1

def nFilter_one_liner_2(filters, msg_structure):
    a = []
    for i in range(0,15):
        a.append((t for t in msg_structure if all(f(t,i) for f in filters)))
    for each_value in a:
        print each_value.next()

if __name__ == "__main__":
    number_of_lines(17)
    logger.console("location master content has {0}".format(msg_structure))
    create_txt_file(msg_structure)
    serialize_msg_structure()
    line1 = msg_structure[0]
    logger.console(line1.serialize())
    deserialize_msg_structure()
    logger.console (msg_structure_reload)
    create_txt_file(msg_structure_reload, "LOCATION-MASTER-DESERIALIZED.TXT")
    compare_message_before_after()
    my_filtered_msg_structure = filter_using_non_lambda_way([f1, f2], msg_structure)
    logger.console(my_filtered_msg_structure)
    create_txt_file(my_filtered_msg_structure, "LOCATION-MASTER-FILTER1.TXT")
    my_filtered_msg_structure = nFilter_one_liner([f1, f2], msg_structure)
    # when a generator is created, then, headers could not be properly handled!
    create_txt_file(my_filtered_msg_structure,"LOCATION-MASTER-FILTER2.TXT" )
    validate_input_output()
    nFilter_one_liner_2([f3, f4], msg_structure)

