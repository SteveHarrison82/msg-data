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
        "STREET": {
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

order_header = ["PLANT_ID", "PLANT_DESC", "FACTORY_CALENDAR_ID", "STREET", "REGION_CODE", "COUNTRY_CODE", "ZIPCODE", "BLKD_STOCK_RLV_FLAG"]

import json
import python_jsonschema_objects as pjs
from io import StringIO


read_schema = StringIO(location_master)
spec = json.load(read_schema)

builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
LocMaster =  ns.LocationMaster
msg_structure = []
import csv

import random, string
from robot.api import logger


def random_int(length):
    valid_letters='1234567890'
    return ''.join((random.choice(valid_letters) for i in xrange(length)))

def random_word(length):
   return ''.join(random.choice(string.lowercase) for i in range(length))

def enrich_msg_lines(gen_line_with_attribute):
    gen_line_with_attribute.PLANT_ID = random_int(4)
    gen_line_with_attribute.PLANT_DESC = gen_line_with_attribute.PLANT_ID  + random_word(25)
    gen_line_with_attribute.FACTORY_CALENDAR_ID = random_word(2)
    gen_line_with_attribute.VENDOR_ID = random_word(10)
    gen_line_with_attribute.STREET = random_word(12) + ' ' + random_word(10)
    gen_line_with_attribute.CITY = random_word(20)
    gen_line_with_attribute.REGION_CODE = random_word (20)
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

def make_csv(cdr_list, file_name='LOCATION-MASTER.TXT'):
    with open(file_name, 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter="|")
        wr.writerow(cdr_list[0].keys())
        for cdr in cdr_list:
            wr.writerow(cdr.values())

    with open('LOCATION-MASTER.TXT', 'rb') as i:
        with open('LOCATION-MASTER.TXT', 'wb') as o:
            read_csv = csv.DictReader(i,  delimiter='|')
            write_csv = csv.DictWriter(o, order_header, extrasaction='ignore',  delimiter='|')
            write_csv.writeheader()
            for read_row in read_csv:
                write_csv.writerow(read_row)

if __name__ == "__main__":
    number_of_lines(12)
    logger.console("location master content has {0}".format(msg_structure))
    make_csv(msg_structure)










