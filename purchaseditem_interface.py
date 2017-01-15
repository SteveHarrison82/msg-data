# -*- coding: utf-8 -*-

import json
import python_jsonschema_objects as pjs
from io import StringIO
import pandas as pd
import csv
import random, string
from robot.api import logger
import pickle

purchased_item = u"""{
    "title": "Purchased Item",
    "type": "object",
    "properties": {
        "MATERIAL_ID": {
            "position": "0",
            "type": "string"
        },
        "PLANT_ID": {
            "position": "1",
            "type": "string"
        },
        "PURCHASING_GROUP_ID": {
            "position": "2",
            "type": "string"
            },
        "MATERIAL_TYPE": {
            "description": "calendars",
            "position": "3",
            "type": "string"
            },
        "BASE_UOM": {
            "description": "calendars",
            "position": "4",
            "type": "string"
            },
        "MATERIAL_DESC": {
            "position": "5",
            "type": "string"
        },
        "VENDOR_ID": {
            "position": "6",
            "type": "string"
        },
        "VENDOR_MATERIAL_ID": {
            "position": "7",
            "type": "string"
        },
        "INFREC_ID": {
            "position": "8",
            "type": "string"
        },
        "INFREC_TYPE": {
            "position": "9",
            "type": "string"
        },
        "PURCHASING_ORG_ID": {
            "position": "10",
            "type": "string"
        },
        "DLV_TIME_DAYS": {
            "position": "11",
            "type": "string"
        },
        "MIN_PURCHORD_QTY": {
            "position": "12",
            "type": "string"
        }
    },
    "required": ["MATERIAL_ID", "PLANT_ID", "PURCHASING_GROUP_ID", "MATERIAL_TYPE", "BASE_UOM", "MATERIAL_DESC", "VENDOR_ID","INFREC_ID","INFREC_TYPE", "PURCHASING_ORG_ID", "DLV_TIME_DAYS", "MIN_PURCHORD_QTY"]
}"""


position_of_PI_header = ["MATERIAL_ID", "PLANT_ID", "PURCHASING_GROUP_ID", "MATERIAL_TYPE", "BASE_UOM", "MATERIAL_DESC", "VENDOR_ID",
                "VENDOR_MATERIAL_ID", "INFREC_ID", "INFREC_TYPE", "PURCHASING_ORG_ID", "DLV_TIME_DAYS", "MIN_PURCHORD_QTY"]



read_schema = StringIO(purchased_item)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
PurchItem = ns.PurchasedItem
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
    gen_line_with_attribute.MATERIAL_ID = random_pick(['000000000000001108', '100-100', '000000000000001268', '000000000000001267'])
    gen_line_with_attribute.PLANT_ID = random_pick (['7500', '3000', '1000', '1100', '1200', '2000'])
    gen_line_with_attribute.PURCHASING_GROUP_ID = random_pick (['AR1', '001', '100'])
    gen_line_with_attribute.MATERIAL_TYPE = random_pick (['HALB', 'ROH', 'HALB', 'HALB'])
    gen_line_with_attribute.BASE_UOM = random_pick (['ST', 'KG', 'M2', 'EA'])
    gen_line_with_attribute.MATERIAL_DESC = "Desc : " + gen_line_with_attribute.PLANT_ID
    gen_line_with_attribute.VENDOR_ID = random_pick(['000000', '000000', '000000', '000000']) + random_int(4)
    gen_line_with_attribute.VENDOR_MATERIAL_ID = random_pick(['FC-1000', 'RESERVOIR/BRUT/CX', 'FC-1000', 'PROTEZIONE P3', '', '', '', ''])
    gen_line_with_attribute.INFREC_ID = random_pick(['5300005','5300005','5300005']) + random_int(3)
    gen_line_with_attribute.INFREC_TYPE = random_pick(['FC-1000', 'RESERVOIR/BRUT/CX', 'FC-1000', 'PROTEZIONE P3', '', '', '', ''])#random_int(10) 5300004936
    gen_line_with_attribute.PURCHASING_ORG_ID = random_pick(['3000', '1000', '2200', '1000', '3000'])
    gen_line_with_attribute.DLV_TIME_DAYS = random_pick(['0', '0', '10', '5', '1', '3', '90', '10'])
    gen_line_with_attribute.MIN_PURCHORD_QTY = random_pick(['0', '0', '0','0', '0', '0','0', '0', '0','0', '0', '0','0', '0', '0','2600'])
    logger.console("Generate line is {0}".format(gen_line_with_attribute))
    return gen_line_with_attribute



def create_txt_file(msg_structure, file_name='PURCHASED-ITEMS.TXT'):
    with open('PURCHASED-ITEMS.TXT-UNORDERED', 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter="|")
        wr.writerow(msg_structure[0].keys())
        #msg_structure.sort(key=lambda x: (x.PLANT_ID, x.STREET), reverse=False)
        for msg_line in msg_structure:
            wr.writerow(msg_line.values())

    with open('PURCHASED-ITEMS.TXT-UNORDERED', 'rb') as input_file:
        with open(file_name, 'wb') as output_file:
            read_csv = csv.DictReader(input_file, delimiter='|')
            # save ignoring certain columns by using list: position_of_header or position_of_header_subset
            write_csv = csv.DictWriter(output_file, position_of_PI_header, delimiter='|', extrasaction='ignore')
            write_csv.writeheader()
            for read_row in read_csv:
                write_csv.writerow(read_row)

def msgline_to_list(msg_line):
    global msg_structure
    logger.console("Adding msg line to msg_structure")
    msg_structure.append(msg_line)
    create_txt_file(msg_structure)


def serialize_msg_structure():
    with open('PURCHASED-ITEMS.ser', 'wb') as f:
        # order of msg_structure is changed as it is sorted in place
        for each_msg_line in msg_structure:
            pickle.dump(each_msg_line.as_dict(), f)

#messageIO
#load from file
def deserialize_msg_structure():
    objects = []
    global msg_structure_reload;
    with open('PURCHASED-ITEMS.ser', 'rb') as openfile:
        while True:
            try:
                objects.append(pickle.load(openfile))
            except EOFError:
                break
    for each_obj in objects:
        msg_line_as_json = json.dumps(each_obj)
        create_line_on_deserialization = PurchItem.from_json(msg_line_as_json)
        msg_structure_reload.append(create_line_on_deserialization)

def create_zip_file():
    pass

def number_of_lines(lines_to_generate=1):
    for each_line in range(lines_to_generate):
        gen_line_with_attribute = PurchItem()
        gen_line_with_attribute = enrich_msg_lines(gen_line_with_attribute)
        msgline_to_list(gen_line_with_attribute)
        serialize_msg_structure()
        create_zip_file()

# Enterprise Code	Site Name	Supplier ID	Customer Item ID	Customer Item Description

purchased_item_ssp = u"""{
    "title": "Purchased Itemssp",
    "type": "object",
    "properties": {
        "Enterprise Code": {
            "position": "0",
            "type": "string"
        },
        "Site Name": {
            "position": "1",
            "type": "string"
        },
        "Supplier ID": {
            "position": "1",
            "type": "string"
            },
        "Customer Item ID": {
            "position": "3",
            "type": "string"
            },
        "Customer Item Description": {
            "position": "4",
            "type": "string"
            }
    }
}"""

read_schema = StringIO(purchased_item_ssp)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
pi_ssp = ns.PurchasedItemssp

pi_ssp_message_structure = []

def enrich_msg_lines_pi(gen_line_with_attribute, zip_line):
    gen_line_with_attribute["Enterprise Code"] = zip_line[0]
    gen_line_with_attribute["Site Name"] = zip_line[1]
    gen_line_with_attribute["Supplier ID"] = zip_line[2]
    gen_line_with_attribute["Customer Item ID"] = zip_line[3]
    gen_line_with_attribute["Customer Item Description"] = zip_line[4]
    pi_ssp_message_structure.append(gen_line_with_attribute)
    return pi_ssp_message_structure

gen_line_with_attribute_supplier = pi_ssp()

def read_pi_data():
    global gen_line_with_attribute_supplier
    loc_file = open("PurchasedItems-1.0", "r")
    loc_file_as_string = loc_file.read()
    d = StringIO(unicode(loc_file_as_string))
    fields = gen_line_with_attribute_supplier.keys()
    df = pd.read_csv(d, usecols=fields, delimiter='\t', dtype=str)
    for index, each_row in df.iterrows():
        data_zipped = []
        data_zipped.append(each_row[0])
        data_zipped.append(each_row[1])
        data_zipped.append(each_row[2])
        data_zipped.append(each_row[3])
        data_zipped.append(each_row[4])
        gen_line_with_attribute_supplier = pi_ssp()
        enrich_msg_lines_pi(gen_line_with_attribute_supplier, data_zipped)

count = 0

def f3(msg_line):
    global count
    return (msg_line.VENDOR_ID == pi_ssp_message_structure[count]["Supplier ID"])

def f4(msg_line):
    global count
    return (msg_line.MATERIAL_ID == pi_ssp_message_structure[count]["Customer Item ID"])

def simple_filter(filters, msg_structure):
    for f in filters:
        msg_structure = filter(f, msg_structure)
        if not msg_structure:
            return msg_structure
    return msg_structure

def validate_input_output():
    deserialize_msg_structure()
    read_pi_data()
    filtered_rows = []
    global msg_structure_reload
    for i in range(0,len(pi_ssp_message_structure)-1):
        global count
        count = count + 1
        msg_structure_filtered = simple_filter([f3, f4], msg_structure_reload)
        if len(msg_structure_filtered) > 0:
            filtered_rows.append (msg_structure_filtered)
    logger.console ("Length of filtered rows are {}".format(len(filtered_rows)))
    assert len(filtered_rows) == count

if __name__ == "__main__":
    #number_of_lines(3)
    #logger.console("location master content has {0}".format(str(msg_structure)))
    #create_txt_file(msg_structure)
    validate_input_output()