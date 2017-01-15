# -*- coding: utf-8 -*-

import json
import python_jsonschema_objects as pjs
from io import StringIO
import csv
import random, string
from robot.api import logger
import pickle
import pandas as pd

vendor_master = u"""{
    "title": "VENDOR MASTER",
    "type": "object",
    "properties": {
        "VENDOR_ID": {
            "position": "0",
            "type": "string"
        },
        "VENDOR_DESC": {
            "position": "1",
            "type": "string"
        },
        "DUNS_NO": {
            "position": "2",
            "type": "string"
            },
        "TAX_NO": {
            "position": "3",
            "type": "string"
            },
        "ADDR_ID": {
            "position": "4",
            "type": "string"
            },
        "STREET": {
            "position": "5",
            "type": "string"
        },
        "CITY": {
            "position": "6",
            "type": "string"
        },
        "STATE_CODE": {
            "position": "7",
            "type": "string"
        },
        "COUNTRY_CODE": {
            "position": "8",
            "type": "string"
        },
        "ZIP_CODE": {
            "position": "9",
            "type": "string"
        },
        "DELETION_FLAG": {
            "position": "10",
            "type": "string"
        },
        "REMITTO_ADDR_ID": {
            "position": "11",
            "type": "string"
        },
        "REMITTO_STREET": {
            "position": "12",
            "type": "string"
        },
        "REMITTO_CITY": {
            "position": "13",
            "type": "string"
        },
        "REMITTO_STATE_CODE": {
            "position": "14",
            "type": "string"
        },
        "REMITTO_COUNTRY_CODE": {
            "position": "15",
            "type": "string"
        },
        "REMITTO_ZIP_CODE": {
            "position": "16",
            "type": "string"
        }
    },
    "required": ["VENDOR_ID"]
}"""


position_of_VM_header = ["VENDOR_ID", "VENDOR_DESC", "DUNS_NO", "ADDR_ID", "STREET", "CITY", "STATE_CODE",
                "COUNTRY_CODE", "ZIP_CODE", "DELETION_FLAG", "REMITTO_ADDR_ID", "REMITTO_STREET",
                "REMITTO_CITY", "REMITTO_STATE_CODE", "REMITTO_COUNTRY_CODE", "REMITTO_ZIP_CODE"]

read_schema = StringIO(vendor_master)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
VenMaster = ns.VendorMaster
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
    gen_line_with_attribute.VENDOR_ID = random_pick(['000000', '000000', '000000', '000000']) + random_int(4)
    gen_line_with_attribute.VENDOR_DESC = "Description : " + gen_line_with_attribute.VENDOR_ID
    gen_line_with_attribute.DUN = random_pick(['210010661', '210010662','','','', '410010667'])
    gen_line_with_attribute.ADDR_ID = '0000' + random_int(6)
    gen_line_with_attribute.STREET = random_pick (['Industriestrasse 19', 'Fasanenstr. 8', '1 Unter den Linden', ])
    gen_line_with_attribute.CITY = random_pick (['Berlin', 'ATLANTA', 'Detroit', 'Delhi'])
    gen_line_with_attribute.STATE_CODE = random_pick(['AZ', 'NZ', 'BC', 'KL', 'MN'])
    gen_line_with_attribute.COUNTRY_CODE = random_pick(['DE', 'US', 'CH', 'FR', 'EN'])
    gen_line_with_attribute.ZIP_CODE = random_int(10)
    gen_line_with_attribute.DELETION_FLAG = random_pick(['X', '', '', '', ''])
    gen_line_with_attribute.REMITTO_ADDR_ID = random_pick(['0000006953', '0000006954', '1000006953', '7000006953'])
    gen_line_with_attribute.REMITTO_STREET = random_pick(['Kolping Str. 15', 'Ersinger straße 23', 'St. Michael stree'])
    gen_line_with_attribute.REMITTO_CITY = random_pick (['Berlin', 'ATLANTA', 'Detroit', 'Delhi'])
    gen_line_with_attribute.REMITTO_STATE_CODE = random_pick(['BZ', 'MZ', 'LC', 'YL', 'KN'])
    gen_line_with_attribute.REMITTO_COUNTRY_CODE = random_pick(['DE', 'US', 'CH', 'IN', 'PK'])
    gen_line_with_attribute.REMITTO_ZIP_CODE = random_int(10)
    logger.console("Generate line is {0}".format(gen_line_with_attribute))
    return gen_line_with_attribute



def create_txt_file(msg_structure, file_name='VENDOR-MASTER.TXT'):
    with open('VENDOR-MASTER.TXT-UNORDERED', 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter="|")
        wr.writerow(msg_structure[0].keys())
        #msg_structure.sort(key=lambda x: (x.PLANT_ID, x.STREET), reverse=False)
        for msg_line in msg_structure:
            wr.writerow(msg_line.values())

    with open('VENDOR-MASTER.TXT-UNORDERED', 'rb') as input_file:
        with open(file_name, 'wb') as output_file:
            read_csv = csv.DictReader(input_file, delimiter='|')
            # save ignoring certain columns by using list: position_of_header or position_of_header_subset
            write_csv = csv.DictWriter(output_file, position_of_VM_header, delimiter='|', extrasaction='ignore')
            write_csv.writeheader()
            for read_row in read_csv:
                write_csv.writerow(read_row)

def msgline_to_list(msg_line):
    global msg_structure
    logger.console("Adding msg line to msg_structure")
    msg_structure.append(msg_line)
    create_txt_file(msg_structure)


def serialize_msg_structure():
    with open('VENDOR-MASTER.ser', 'wb') as f:
        # order of msg_structure is changed as it is sorted in place
        for each_msg_line in msg_structure:
            pickle.dump(each_msg_line.as_dict(), f)

#messageIO
#load from file
def deserialize_msg_structure():
    objects = []
    global msg_structure_reload;
    with open('VENDOR-MASTER.ser', 'rb') as openfile:
        while True:
            try:
                objects.append(pickle.load(openfile))
            except EOFError:
                break
    for each_obj in objects:
        msg_line_as_json = json.dumps(each_obj)
        create_line_on_deserialization = VenMaster.from_json(msg_line_as_json)
        msg_structure_reload.append(create_line_on_deserialization)

def create_zip_file():
    pass

def number_of_lines(lines_to_generate=1):
    for each_line in range(lines_to_generate):
        gen_line_with_attribute = VenMaster()
        gen_line_with_attribute = enrich_msg_lines(gen_line_with_attribute)
        msgline_to_list(gen_line_with_attribute)
        serialize_msg_structure()
        create_zip_file()


supplier = u"""{
    "title": "Supplier",
    "type": "object",
    "properties": {
        "Enterprise Code": {
            "position": "0",
            "type": "string"
        },
        "Supplier ID": {
            "position": "1",
            "type": "string"
        },
         "Supplier Description": {
            "position": "3",
            "type": "string"
            }
    }
}"""

read_schema = StringIO(supplier)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
supplier_vendor = ns.Supplier

supplier_message_structure = []

def enrich_msg_lines_supplier(gen_line_with_attribute, zip_line):
    gen_line_with_attribute["Enterprise Code"]= zip_line[0]
    gen_line_with_attribute["Supplier ID"] = zip_line[1]
    gen_line_with_attribute["Supplier Description"] = zip_line[2]
    supplier_message_structure.append(gen_line_with_attribute)
    return supplier_message_structure

gen_line_with_attribute_supplier = supplier_vendor()

def read_supplier_data():
    global gen_line_with_attribute_supplier
    loc_file = open("Supplier-1.0", "r")
    loc_file_as_string = loc_file.read()
    d = StringIO(unicode(loc_file_as_string))
    fields = gen_line_with_attribute_supplier.keys()
    df = pd.read_csv(d, usecols=fields, delimiter='\t', dtype=str)
    for index, each_row in df.iterrows():
        data_zipped = []
        data_zipped.append(each_row[0])
        data_zipped.append(each_row[1])
        data_zipped.append(each_row[2])
        gen_line_with_attribute_supplier = supplier_vendor()
        enrich_msg_lines_supplier(gen_line_with_attribute_supplier, data_zipped)

count = 0

def f3(msg_line):
    global count
    return (msg_line.VENDOR_ID == supplier_message_structure[count]["Supplier ID"])
def f4(msg_line):
    global count
    return (msg_line.VENDOR_DESC == supplier_message_structure[count]["Supplier Description"])

def simple_filter(filters, msg_structure):
    for f in filters:
        msg_structure = filter(f, msg_structure)
        if not msg_structure:
            return msg_structure
    return msg_structure

def validate_input_output():
    deserialize_msg_structure()
    read_supplier_data()
    filtered_rows = []
    global msg_structure_reload
    for i in range(0,len(supplier_message_structure)-1):
        global count
        count = count + 1
        msg_structure_filtered = simple_filter([f3, f4], msg_structure_reload)
        if len(msg_structure_filtered) > 0:
            filtered_rows.append (msg_structure_filtered)
    logger.console ("Length of filtered rows are {}".format(len(filtered_rows)))
    assert len(filtered_rows) == count

if __name__ == "__main__":
    # number_of_lines(3)
    #logger.console("location master content has {0}".format(str(msg_structure)))
    #create_txt_file(msg_structure)
    validate_input_output()