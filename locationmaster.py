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

order_header = ["PLANT_ID", "PLANT_DESC", "FACTORY_CALENDAR_ID", "VENDOR_ID", "STREET", "CITY", "REGION_CODE",
                "COUNTRY_CODE", "ZIPCODE", "BLKD_STOCK_RLV_FLAG"]
order_header_1 = ["PLANT_ID", "PLANT_DESC", "FACTORY_CALENDAR_ID", "VENDOR_ID", "STREET", "CITY", "REGION_CODE",
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
            # save ignoring certain columns by using list: order_header or order_header_1

            write_csv = csv.DictWriter(output_file, order_header_1, delimiter='|', extrasaction='ignore')
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
    create_txt_file(my_filtered_msg_structure,"LOCATION-MASTER-FILTER2.TXT" )

