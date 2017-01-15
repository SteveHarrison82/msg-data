# -*- coding: utf-8 -*-

import csv
from robot.api import logger
import pickle
import json
import python_jsonschema_objects as pjs
from io import StringIO
import pandas as pd
import random, string

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
            "position": "3",
            "type": "string"
            },
        "STREET": {
            "position": "4",
            "type": "string"
        },
        "CITY": {
            "position": "5",
            "type": "string"
        },
        "STATE_CODE": {
            "position": "6",
            "type": "string"
        },
        "COUNTRY_CODE": {
            "position": "7",
            "type": "string"
        },
        "ZIP_CODE": {
            "position": "8",
            "type": "string"
        },
        "BLKD_STOCK_RLV_FLAG": {
            "position": "9",
            "type": "string"
        }
    },
    "required": ["PLANT_ID"]
}"""


position_of_LM_header = ["PLANT_ID", "PLANT_DESC", "FACTORY_CALENDAR_ID", "VENDOR_ID", "STREET", "CITY", "STATE_CODE",
                "COUNTRY_CODE", "ZIP_CODE", "BLKD_STOCK_RLV_FLAG"]



read_schema = StringIO(location_master)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
LocMaster = ns.LocationMaster
msg_structure = []
msg_structure_reload = []


def random_int(length):
    valid_letters = '1234567890'
    return ''.join((random.choice(valid_letters) for i in xrange(length)))


def random_word(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def random_pick(choices):
    return random.choice(choices)

#create message lines with these attributes
def enrich_msg_lines(gen_line_with_attribute):
    gen_line_with_attribute.PLANT_ID = random_pick(['0001','0005','0006','0007'])
    gen_line_with_attribute.PLANT_DESC = gen_line_with_attribute.PLANT_ID + "Description here: " + random_pick(['Werk','Hamburg', 'New York' ])
    gen_line_with_attribute.FACTORY_CALENDAR_ID = random_pick(['01', 'US', '69', 'FR', 'CA', 'JP'])
    gen_line_with_attribute.VENDOR_ID = random_pick(['0000000001', '0000000002', '0000000003', '0000000004'])
    gen_line_with_attribute.STREET = random_pick(['Alsterdorfer Strasse 13','Neurottstrass 16','Pillnitzer Strasse 241','St. Michael street'])
    gen_line_with_attribute.CITY = random_pick(['Paris','Berlin','Frankfurt','Hamburg'])
    gen_line_with_attribute.STATE_CODE = random_pick(['Wunderland', 'European park', 'Hethbrew'])
    gen_line_with_attribute.COUNTRY_CODE = random_int(3)
    gen_line_with_attribute.ZIP_CODE = random_int(10)
    gen_line_with_attribute.BLKD_STOCK_RLV_FLAG = 'X'
    logger.console("Generate line is {0}".format(LocMaster))
    return gen_line_with_attribute

def msgline_to_list(msg_line):
    global msg_structure
    logger.console("Adding msg line to msg_structure")
    msg_structure.append(msg_line)
    create_txt_file(msg_structure)

def create_txt_file(msg_structure, file_name='LOCATION-MASTER.TXT'):
    with open('LOCATION-MASTER.TXT-UNORDERED', 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter="|")
        wr.writerow(msg_structure[0].keys())
        #msg_structure.sort(key=lambda x: (x.PLANT_ID, x.STREET), reverse=False)
        for msg_line in msg_structure:
            wr.writerow(msg_line.values())

    with open('LOCATION-MASTER.TXT-UNORDERED', 'rb') as input_file:
        with open(file_name, 'wb') as output_file:
            read_csv = csv.DictReader(input_file, delimiter='|')
            # save ignoring certain columns by using list: position_of_header or position_of_header_subset
            write_csv = csv.DictWriter(output_file, position_of_LM_header, delimiter='|', extrasaction='ignore')
            write_csv.writeheader()
            for read_row in read_csv:
                write_csv.writerow(read_row)

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

def create_zip_file():
    pass

def number_of_lines(lines_to_generate=1):
    for each_line in range(lines_to_generate):
        gen_line_with_attribute = LocMaster()
        gen_line_with_attribute = enrich_msg_lines(gen_line_with_attribute)
        msgline_to_list(gen_line_with_attribute)
        serialize_msg_structure()
        create_zip_file()

hub = u"""{
    "title": "Hub",
    "type": "object",
    "properties": {
        "Enterprise Code": {
            "position": "0",
            "type": "string"
        },
        "Enterprise Description": {
            "position": "1",
            "type": "string"
        },
        "Site Name": {
            "position": "2",
            "type": "string"
            },
        "Site Description": {
            "position": "3",
            "type": "string"
            },
        "Site Type": {
            "position": "4",
            "type": "string"
        }
    }
}"""


read_schema = StringIO(hub)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
hub = ns.Hub
hub_message_structure = []

def enrich_msg_lines_hub(gen_line_with_attribute, zip_line):
    gen_line_with_attribute["Enterprise Code"] = zip_line[0]
    gen_line_with_attribute["Enterprise Description"] = zip_line[1]
    gen_line_with_attribute["Site Name"] = zip_line[2]
    gen_line_with_attribute["Site Description"] = zip_line[3]
    gen_line_with_attribute["Site Type"] = zip_line[4]
    hub_message_structure.append(gen_line_with_attribute)
    return hub_message_structure

gen_line_with_attribute_hub = hub()

def read_hub_data():
    global gen_line_with_attribute_hub
    loc_file = open("Hub-1.0", "r")
    logger.console ("reading file Hub-1.0")
    loc_file_as_string = loc_file.read()
    logger.console ("reding file ..")
    d = StringIO(unicode(loc_file_as_string))
    logger.console ("table as unicode string is {}".format(d))
    fields = gen_line_with_attribute_hub.keys()
    df = pd.read_csv(d, usecols=fields, delimiter='\t', dtype=str)
    for index, each_row in df.iterrows():
        data_zipped = []
        data_zipped.append(each_row["Enterprise Code"])
        data_zipped.append(each_row["Enterprise Description"])
        data_zipped.append(each_row["Site Name"])
        data_zipped.append(each_row["Site Description"])
        data_zipped.append(each_row["Site Type"])
        gen_line_with_attribute_hub = hub()
        enrich_msg_lines_hub(gen_line_with_attribute_hub, data_zipped)

count = 0

def f3(msg_line):
    global count

    return (msg_line.PLANT_ID == hub_message_structure[count]["Site Name"])

def f4(msg_line):
    global count

    return (msg_line.PLANT_DESC == hub_message_structure[count]["Site Description"])

def simple_filter(filters, msg_structure):
    for f in filters:
        msg_structure = filter(f, msg_structure)
        if not msg_structure:
            return msg_structure
    return msg_structure

def validate_input_output():
    deserialize_msg_structure()
    read_hub_data()
    filtered_rows = []
    global msg_structure_reload
    logger.console ("length of hub msg lines to verify {}".format(len(hub_message_structure)))
    for i in range(0,len(hub_message_structure)-1):
        global count
        count = count + 1
        logger.info ("validating data {}".format(count))
        logger.info(hub_message_structure[count]["Site Name"])
        logger.info(hub_message_structure[count]["Site Description"])
        msg_structure_filtered = simple_filter([f3, f4], msg_structure_reload)
        if len(msg_structure_filtered) > 0:
            filtered_rows.append (msg_structure_filtered)
    logger.console ("Length of filtered rows are {}".format(len(filtered_rows)))
    assert len(filtered_rows) == count

if __name__ == "__main__":
    #number_of_lines(4)
    #logger.console("location master content has {0}".format(msg_structure))
    #create_txt_file(msg_structure)
    # wait for the transaction to end
    validate_input_output()