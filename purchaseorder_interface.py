# -*- coding: utf-8 -*-

import json
import python_jsonschema_objects as pjs
from io import StringIO
import numpy
import csv
import random, string
from robot.api import logger
import pickle

purchase_order = u"""{
    "title": "Purchase Order",
    "type": "object",
    "properties": {
        "ORDER_ID": {
            "position": "0",
            "type": "string"
        },
        "ORDER_TYPE": {
            "position": "1",
            "type": "string"
        },
        "CUSTOMER_ID": {
            "position": "2",
            "type": "string"
            },
        "SUPPLIER_ID": {
            "position": "3",
            "type": "string"
            },
        "ORDER_CREATION_DATE": {
            "position": "4",
            "type": "string"
            },
        "ITEM_NO": {
            "position": "5",
            "type": "string"
        },
        "MATERIAL_ID": {
            "position": "6",
            "type": "string"
        },
        "MATERIAL_REVISION": {
            "position": "7",
            "type": "string"
        },
        "SUPPLIER_ITEM_NAME": {
            "position": "8",
            "type": "string"
        },
        "ITEM_QTY": {
            "position": "9",
            "type": "string"
        },
        "ITEM_UOM": {
            "position": "10",
            "type": "string"
        },
        "UNIT_PRICE": {
            "position": "11",
            "type": "string"
        },
        "CURRENCY_CODE": {
            "position": "12",
            "type": "string"
        },
        "ITEM_SHIP_TO_PARTY_ID": {
            "position": "13",
            "type": "string"
        },
        "SCH_NO": {
            "position": "14",
            "type": "string"
        },
        "SCH_REQUESTED_QTY": {
            "position": "14",
            "type": "string"
        },
        "SCH_REQUESTED_DATE": {
            "position": "14",
            "type": "string"
        }

    },
    "required": ["ORDER_ID", "ITEM_NO", "SCH_NO"]
}"""


position_of_PO_header = ["ORDER_ID", "ORDER_TYPE", "CUSTOMER_ID", "SUPPLIER_ID", "ORDER_CREATION_DATE", "ITEM_NO", "MATERIAL_ID",
                         "MATERIAL_REVISION","SUPPLIER_ITEM_NAME", "ITEM_QTY", "ITEM_UOM", "UNIT_PRICE",
                         "CURRENCY_CODE", "ITEM_SHIP_TO_PARTY_ID", "SCH_NO", "SCH_REQUESTED_QTY","SCH_REQUESTED_DATE"]

read_schema = StringIO(purchase_order)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
PurchOrd = ns.PurchaseOrder
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
    gen_line_with_attribute["ORDER_ID"] = random_pick(['7712345', '7712345', '7712345', '7712345'])+ random_int(7)
    gen_line_with_attribute["ORDER_TYPE"] = random_pick (['NB', 'NB'])
    gen_line_with_attribute["CUSTOMER_ID"] = random_pick (['1000', '1000', '2000'])
    gen_line_with_attribute["SUPPLIER_ID"] = random_pick (['0000003730', '0000003730', '0000003730', '0000003730'])
    gen_line_with_attribute["ORDER_CREATION_DATE"] = random_pick (['2016-02-05', '2016-03-05', '2016-05-05', '2016-09-09'])
    gen_line_with_attribute["MATERIAL_ID"]= random_pick (['MSA-2000', 'R-1002', 'MSA-2006', 'WL-1000', '100-400', 'DG-1000', 'I-1100', 'MSA-2006'])
    gen_line_with_attribute["ITEM_NO"] = random_pick(['00004', '00003', '00002', '00001'])
    gen_line_with_attribute["MATERIAL_REVISION"] = random_pick(['00000', '00000', '00000', '00000']) + random_int(5)
    gen_line_with_attribute["SUPPLIER_ITEM_NAME"] = random_pick(['MSA-2000-3730', 'MSA-2006-3730', '100-400-3411', 'I-1100-3411', 'MSA-2006-3730', 'MSA-2006-3730', 'MSA-2006-3730', 'MSA-2006-3730'])
    gen_line_with_attribute["ITEM_QTY"] = random_pick(['1','2','1','1','2','1','4','2','6'])
    gen_line_with_attribute["ITEM_UOM"] = random_pick(['ST', 'ST', 'ST', 'ST', 'ST', 'ST', 'ST', 'ST'])
    gen_line_with_attribute["UNIT_PRICE"] = random_pick(['3000', '1000', '2200', '1000', '3000'])
    gen_line_with_attribute["CURRENCY_CODE"] = random_pick(['USD', 'EUR', 'INR', 'USD', 'USD', 'USD', 'EUR', 'EUR'])
    gen_line_with_attribute["ITEM_SHIP_TO_PARTY_ID"] = random_pick(['3200', '3200', '3200','3200', '3200'])
    gen_line_with_attribute["SCH_NO"] = random_pick(['0001', '0001', '0001', ])
    gen_line_with_attribute["SCH_REQUESTED_QTY"] = random_pick(['1', '3', '2', '7', '6', '1'])
    gen_line_with_attribute["SCH_REQUESTED_DATE"] = random_pick(['2017-02-05', '2017-03-05', '2017-05-05', '2017-09-09'])
    logger.console("Generate line is {0}".format(gen_line_with_attribute))
    return gen_line_with_attribute

def create_txt_file(msg_structure, file_name='PURCHASE-ORDER.TXT'):
    with open('PURCHASE-ORDER.TXT-UNORDERED', 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter="|")
        wr.writerow(msg_structure[0].keys())
        #msg_structure.sort(key=lambda x: (x.PLANT_ID, x.STREET), reverse=False)
        for msg_line in msg_structure:
            wr.writerow(msg_line.values())

    with open('PURCHASE-ORDER.TXT-UNORDERED', 'rb') as input_file:
        with open(file_name, 'wb') as output_file:
            read_csv = csv.DictReader(input_file, delimiter='|')
            write_csv = csv.DictWriter(output_file, position_of_PO_header, delimiter='|', extrasaction='ignore')
            write_csv.writeheader()
            for read_row in read_csv:
                write_csv.writerow(read_row)

def msgline_to_list(msg_line):
    global msg_structure
    logger.console("Adding msg line to msg_structure")
    msg_structure.append(msg_line)
    create_txt_file(msg_structure)

def serialize_msg_structure():
    with open('PURCHASE-ORDER.ser', 'wb') as f:
        # order of msg_structure is changed as it is sorted in place
        for each_msg_line in msg_structure:
            pickle.dump(each_msg_line.as_dict(), f)

#messageIO
#load from file
def deserialize_msg_structure():
    objects = []
    global msg_structure_reload;
    with open('PURCHASE-ORDER.ser', 'rb') as openfile:
        while True:
            try:
                objects.append(pickle.load(openfile))
            except EOFError:
                break
    for each_obj in objects:
        msg_line_as_json = json.dumps(each_obj)
        create_line_on_deserialization = PurchOrd.from_json(msg_line_as_json)
        msg_structure_reload.append(create_line_on_deserialization)

def create_zip_file():
    pass

def number_of_lines(lines_to_generate=1):
    for each_line in range(lines_to_generate):
        gen_line_with_attribute = PurchOrd()
        gen_line_with_attribute = enrich_msg_lines(gen_line_with_attribute)
        msgline_to_list(gen_line_with_attribute)
        serialize_msg_structure()
        create_zip_file()

discrete_order_ssp = u"""{
    "title": "Discrete Order",
    "type": "object",
    "properties": {
        "Order Number": {
            "position": "0",
            "type": "string"
        },
        "Order Creation Date": {
            "position": "1",
            "type": "string"
        },
        "Customer ID": {
            "position": "2",
            "type": "string"
            },
        "Supplier ID": {
            "position": "3",
            "type": "string"
            },
        "Line ID": {
            "position": "4",
            "type": "string"
            },
        "Customer Item ID": {
            "position": "5",
            "type": "string"
            },
        "Supplier Item ID": {
            "position": "6",
            "type": "string"
            },
        "Unit Price": {
            "position": "7",
            "type": "string"
            },
        "Currency": {
            "position": "8",
            "type": "string"
            },
        "Total Line Amount": {
            "position": "9",
            "type": "string"
            },
        "UOM": {
            "position": "10",
            "type": "string"
            },
        "Request ID": {
            "position": "11",
            "type": "string"
            },
        "Action": {
            "position": "12",
            "type": "string"
            },
        "Request Qty": {
            "position": "13",
            "type": "string"
            },
        "Request_Date": {
            "position": "14",
            "type": "string"
            },
        "Customer Site": {
            "position": "15",
            "type": "string"
            }
    }
}"""

read_schema = StringIO(discrete_order_ssp)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
do_ssp = ns.DiscreteOrder

do_ssp_message_structure = []

def enrich_msg_lines_pi(gen_line_with_attribute, zip_line):
    gen_line_with_attribute["Order Number"] = str(zip_line[0])
    gen_line_with_attribute["Order Creation Date"] = str(zip_line[1])
    gen_line_with_attribute["Customer ID"] = str(zip_line[2])
    gen_line_with_attribute["Supplier ID"] = str(zip_line[3])
    gen_line_with_attribute["Line ID"] = str(zip_line[4])
    gen_line_with_attribute["Customer Item ID"] = str(zip_line[5])
    gen_line_with_attribute["Supplier Item ID"] = str(zip_line[6])
    gen_line_with_attribute["Unit Price"] = str(zip_line[7])
    gen_line_with_attribute["Currency"] = str(zip_line[8])
    gen_line_with_attribute["Total Line Amount"] = str(zip_line[9])
    gen_line_with_attribute["UOM"] = str(zip_line[10])
    gen_line_with_attribute["Request ID"] = str(zip_line[11])
    gen_line_with_attribute["Action"] = str(zip_line[12])
    gen_line_with_attribute["Request Qty"] = str(zip_line[13])
    gen_line_with_attribute["Request Date"] = str(zip_line[14])
    gen_line_with_attribute["Customer Site"] = str(zip_line[15])
    do_ssp_message_structure.append(gen_line_with_attribute)
    return do_ssp_message_structure

gen_line_with_attribute_supplier = do_ssp()

import pandas as pd

def read_po_data():
    loc_file = open("DiscreteOrder-1.0", "r")
    loc_file_as_string = loc_file.read()
    d = StringIO(unicode(loc_file_as_string))
    fields = gen_line_with_attribute_supplier.keys()
    df = pd.read_csv(d, usecols=fields, delimiter='\t', dtype=str)
    for index, each_row in df.iterrows():
        data_zipped = []
        data_zipped.append(each_row["Order Number"])
        data_zipped.append(each_row["Order Creation Date"])
        data_zipped.append(each_row["Customer ID"])
        data_zipped.append(each_row["Supplier ID"])
        data_zipped.append(each_row["Line ID"])
        data_zipped.append(each_row["Customer Item ID"])
        data_zipped.append(each_row["Supplier Item ID"])
        data_zipped.append(each_row["Unit Price"])
        data_zipped.append(each_row["Currency"])
        data_zipped.append(each_row["Total Line Amount"])
        data_zipped.append(each_row["UOM"])
        data_zipped.append(each_row["Request ID"])
        data_zipped.append(each_row["Action"])
        data_zipped.append(each_row["Request Qty"])
        data_zipped.append(each_row["Request Date"])
        data_zipped.append(each_row["Customer Site"])
        gen_line_with_attribute_do = do_ssp()
        enrich_msg_lines_pi(gen_line_with_attribute_do, data_zipped)

count = 0

def f3(msg_line):
    global count
    return (msg_line.ORDER_ID == do_ssp_message_structure[count]["Order Number"])
def f4(msg_line):
    global count
    return (msg_line.CUSTOMER_ID == do_ssp_message_structure[count]["Customer ID"])

def simple_filter(filters, msg_structure):
    for f in filters:
        msg_structure = filter(f, msg_structure)
        if not msg_structure:
            return msg_structure
    return msg_structure

def validate_input_output():
    deserialize_msg_structure()
    read_po_data()
    filtered_rows = []
    global msg_structure_reload
    for i in range(0,len(do_ssp_message_structure)-1):
        global count
        count = count + 1
        msg_structure_filtered = simple_filter([f3, f4], msg_structure_reload)
        if len(msg_structure_filtered) > 0:
            filtered_rows.append (msg_structure_filtered)
    logger.console ("Length of filtered rows are {}".format(len(filtered_rows)))
    assert len(filtered_rows) == count

if __name__ == "__main__":
    number_of_lines(3)
    #logger.console("location master content has {0}".format(str(msg_structure)))
    #create_txt_file(msg_structure)
    #validate_input_output()