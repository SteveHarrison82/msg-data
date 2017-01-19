# -*- coding: utf-8 -*-

import csv
from robot.api import logger
import pickle
import json
import python_jsonschema_objects as pjs
from io import StringIO
import pandas as pd
import random, string


goods_receipt = u"""{
    "title": "Goods Receipt",
    "type": "object",
    "properties": {
        "CREATION_DATE": {
            "position": "0",
            "type": "string"
        },
        "CUSTOMER_ID": {
            "position": "1",
            "type": "string"
        },
        "DOCUMENT_NO": {
            "description": "calendars",
            "position": "2",
            "type": "string"
            },
        "DOCUMENT_YEAR": {
            "position": "3",
            "type": "string"
            },
        "PLANT_ID": {
            "position": "4",
            "type": "string"
        },
        "VENDOR_ID": {
            "position": "5",
            "type": "string"
        },
        "MATERIAL_ID": {
            "position": "6",
            "type": "string"
        },
        "MOVEMENT_TYPE_DESC": {
            "position": "7",
            "type": "string"
        },
        "PURCHASE_ORDER_ID": {
            "position": "8",
            "type": "string"
        },
        "PURCHASE_ORDER_ITEM_NO": {
            "position": "9",
            "type": "string"
        },
        "QUANTITY": {
            "position": "10",
            "type": "string"
        },
        "REVERSAL_FLAG": {
            "position": "11",
            "type": "string"
        },
        "STOCK_TYPE_DESC": {
            "position": "12",
            "type": "string"
        },
        "UOM": {
            "position": "13",
            "type": "string"
        }
    },
    "required": ["PLANT_ID"]
}"""

# CREATION_DATE
# CUSTOMER_ID
# DOCUMENT_NO
# DOCUMENT_YEAR
# PLANT_ID
# VENDOR_ID
# ITEM_NO
# MATERIAL_ID
# MOVEMENT_TYPE_DESC
# MOVEMENT_TYPE_ID
# PURCHASE_ORDER_ID
# PURCHASE_ORDER_ITEM_NO
# QUANTITY
# REVERSAL_FLAG
# STOCK_TYPE_DESC
# UOM

position_of_GR_header = ["CREATION_DATE","CUSTOMER_ID","DOCUMENT_NO","PLANT_ID","VENDOR_ID","ITEM_NO","MATERIAL_ID","MOVEMENT_TYPE_DESC","MOVEMENT_TYPE_ID",
"PURCHASE_ORDER_ID","PURCHASE_ORDER_ITEM_NO","QUANTITY","REVERSAL_FLAG","STOCK_TYPE_DESC","UOM"]

read_schema = StringIO(goods_receipt)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
LocMaster = ns.GoodsReceipt
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
    gen_line_with_attribute.CREATION_DATE = random_pick(['2000-12-19T00:00:00+0000', '2012-11-20T00:00:00+0000', '2012-09-21T00:00:00+0000', '2012-10-22T00:00:00+0000'])
    gen_line_with_attribute.CUSTOMER_ID = random_int(12)
    gen_line_with_attribute.DOCUMENT_NO = '50000' + random_int(5)
    gen_line_with_attribute.VENDOR_ID = random_pick(['000000', '000000', '000000', '000000']) + random_int(4)
    gen_line_with_attribute.PLANT_ID = random_pick(['2000','0001','0005','0006','0007','1000'])
    gen_line_with_attribute.ITEM_NO = '000' + random_int(1)
    gen_line_with_attribute.MATERIAL_ID = random_pick(['MSA-2000', 'MSA-2001', 'MSA-2002', '100-101', '100-100', '112-102'])
    gen_line_with_attribute.MOVEMENT_TYPE_DESC = "Provide material discription"
    gen_line_with_attribute.MOVEMENT_TYPE_ID = random_pick(['101', '102', '103'])
    gen_line_with_attribute.PURCHASE_ORDER_ID = random_int(5) + random_pick(['30000','40000','50000'])
    gen_line_with_attribute.PURCHASE_ORDER_ITEM_NO = random_pick(['00001', '00011', '00004', '00005'])
    gen_line_with_attribute.QUANTITY = random_int(5)
    gen_line_with_attribute.REVERSAL_FLAG =  "0"
    gen_line_with_attribute.STOCK_TYPE_DESC = "Provide stock type desc"
    gen_line_with_attribute.UOM =  "ST"
    logger.console("Generate line is {0}".format(LocMaster))
    return gen_line_with_attribute

def msgline_to_list(msg_line):
    global msg_structure
    logger.console("Adding msg line to msg_structure")
    msg_structure.append(msg_line)
    create_txt_file(msg_structure)

def create_txt_file(msg_structure, file_name='GoodsReceipt.TXT'):
    with open('GoodsReceipt.TXT-UNORDERED', 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter="|")
        wr.writerow(msg_structure[0].keys())
        #msg_structure.sort(key=lambda x: (x.PLANT_ID, x.STREET), reverse=False)
        for msg_line in msg_structure:
            wr.writerow(msg_line.values())

    with open('GoodsReceipt.TXT-UNORDERED', 'rb') as input_file:
        with open(file_name, 'wb') as output_file:
            read_csv = csv.DictReader(input_file, delimiter='|')
            # save ignoring certain columns by using list: position_of_header or position_of_header_subset
            write_csv = csv.DictWriter(output_file, position_of_GR_header, delimiter='|', extrasaction='ignore')
            write_csv.writeheader()
            for read_row in read_csv:
                write_csv.writerow(read_row)

def serialize_msg_structure():
    with open('GoodsReceipt.ser', 'wb') as f:
        # order of msg_structure is changed as it is sorted in place
        for each_msg_line in msg_structure:
            pickle.dump(each_msg_line.as_dict(), f)

#messageIO
#load from file
def deserialize_msg_structure():
    objects = []
    global msg_structure_reload;
    with open('GoodsReceipt.ser', 'rb') as openfile:
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

receipt = u"""{
    "title": "Goods ReceiptSSP",
    "type": "object",
    "properties": {
        "Receipt ID": {
            "position": "0",
            "type": "string"
        },
        "Customer ID": {
            "position": "1",
            "type": "string"
        },
        "Supplier ID": {
            "position": "2",
            "type": "string"
            },
        "Receipt Creation Date": {
            "position": "3",
            "type": "string"
            },
        "Received At (Hub or Site)": {
            "position": "4",
            "type": "string"
            },
        "Receipt Date (Hdr)": {
            "position": "5",
            "type": "string"
            },
        "Receiving Site": {
            "position": "6",
            "type": "string"
            },
        "Receipt Line Id": {
            "position": "7",
            "type": "string"
            },
        "Action": {
            "position": "8",
            "type": "string"
            },
        "Customer Item ID": {
            "position": "9",
            "type": "string"
            },
        "Receipt Date": {
            "position": "10",
            "type": "string"
            },
        "Receipt Quantity": {
            "position": "11",
            "type": "string"
            },
        "UOM": {
            "position": "12",
            "type": "string"
            },
        "Ref Order Type": {
            "position": "13",
            "type": "string"
            },
        "Ref Order ID": {
            "position": "14",
            "type": "string"
            },
        "Ref Order Line ID": {
            "position": "15",
            "type": "string"
            },
        "Movement Type Ref": {
            "position": "16",
            "type": "string"
            }
    }
}"""

#---------------------#
#  Interface content
#---------------------#
# Receipt ID
# Customer ID
# Supplier ID
# Receipt Creation Date
# Received At (Hub or Site)
# Receipt Date (Hdr)
# Receiving Site
# Receipt Line Id
# Action
# Customer Item ID
# Receipt Date
# Receipt Quantity
# UOM
# Ref Order Type
# Ref Order ID
# Ref Order Line ID
# Movement Type Ref


read_schema = StringIO(receipt)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
goodsreceipt_ssp = ns.GoodsReceiptssp
goodsreceipt_ssp_message_structure = []

def enrich_msg_lines_hub(gen_line_with_attribute, zip_line):
    gen_line_with_attribute["Receipt ID"] = str(zip_line[0])
    gen_line_with_attribute["Customer ID"] = str(zip_line[1])
    gen_line_with_attribute["Supplier ID"] = str(zip_line[2])
    gen_line_with_attribute["Receipt Creation Date"] = str(zip_line[3])
    gen_line_with_attribute["Received At (Hub or Site)"] = str(zip_line[4])
    gen_line_with_attribute["Receipt Date (Hdr)"] = str(zip_line[5])
    gen_line_with_attribute["Receiving Site"] = str(zip_line[6])
    gen_line_with_attribute["Receipt Creation Date"] = str(zip_line[7])
    gen_line_with_attribute["Receipt Line Id"] = str(zip_line[8])
    gen_line_with_attribute["Action"] = str(zip_line[9])
    gen_line_with_attribute["Customer Item ID"] = str(zip_line[10])
    gen_line_with_attribute["Receipt Date"] = str(zip_line[11])
    gen_line_with_attribute["Receipt Quantity"] = str(zip_line[12])
    gen_line_with_attribute["UOM"] = str(zip_line[14])
    gen_line_with_attribute["Ref Order Type"] = str(zip_line[14])
    gen_line_with_attribute["Ref Order Line ID"] = str(zip_line[15])
    gen_line_with_attribute["Movement Type Ref"] = str(zip_line[16])
    goodsreceipt_ssp_message_structure.append(gen_line_with_attribute)
    return goodsreceipt_ssp_message_structure

gen_line_with_attribute_hub = goodsreceipt_ssp()

def read_hub_data():
    global gen_line_with_attribute_hub
    loc_file = open("Receipt-1.0", "r")
    logger.console ("reading file Receipt-1.0")
    loc_file_as_string = loc_file.read()
    logger.console ("reding file ..")
    d = StringIO(unicode(loc_file_as_string))
    logger.console ("table as unicode string is {}".format(d))
    fields = gen_line_with_attribute_hub.keys()
    df = pd.read_csv(d, usecols=fields, delimiter='\t', dtype=str)
    for index, each_row in df.iterrows():
        data_zipped = []
        data_zipped.append(each_row["Receipt ID"])
        data_zipped.append(each_row["Customer ID"])
        data_zipped.append(each_row["Supplier ID"])
        data_zipped.append(each_row["Receipt Creation Date"])
        data_zipped.append(each_row["Received At (Hub or Site)"])
        data_zipped.append(each_row["Receipt Date (Hdr)"])
        data_zipped.append(each_row["Receiving Site"])
        data_zipped.append(each_row["Receipt Creation Date"])
        data_zipped.append(each_row["Receipt Line Id"])
        data_zipped.append(each_row["Action"])
        data_zipped.append(each_row["Customer Item ID"])
        data_zipped.append(each_row["Receipt Date"])
        data_zipped.append(each_row["Receipt Quantity"])
        data_zipped.append(each_row["UOM"])
        data_zipped.append(each_row["Ref Order Line ID"])
        data_zipped.append(each_row["Movement Type Ref"])
        enrich_msg_lines_hub(gen_line_with_attribute_hub, data_zipped)

count = 0

def f3(msg_line):
    global count

    return (msg_line.PLANT_ID == goodsreceipt_ssp_message_structure[count]["Customer ID"])

def f4(msg_line):
    global count

    return (msg_line.PURCHASE_ORDER_ID == goodsreceipt_ssp_message_structure[count]["Ref Order ID"])

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
    logger.console ("length of hub msg lines to verify {}".format(len(goodsreceipt_ssp_message_structure)))
    for i in range(0,len(goodsreceipt_ssp_message_structure)-1):
        global count
        count = count + 1
        logger.info ("validating data {}".format(count))
        msg_structure_filtered = simple_filter([f3, f4], msg_structure_reload)
        if len(msg_structure_filtered) > 0:
            filtered_rows.append (msg_structure_filtered)
    logger.console ("Length of filtered rows are {}".format(len(filtered_rows)))
    assert len(filtered_rows) == count

if __name__ == "__main__":
    number_of_lines(4)
    #logger.console("location master content has {0}".format(msg_structure))
    #create_txt_file(msg_structure)
    # wait for the transaction to end
    #validate_input_output()