import json
import python_jsonschema_objects as pjs
from io import StringIO
import csv
import datetime
from itertools import ifilter
import random, string
from robot.api import logger
import pickle
import json
import python_jsonschema_objects as pjs
from io import StringIO
import numpy
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
            "description": "calendars",
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
# Uset the above json spec to build the template class and corresponding object
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



read_schema = StringIO(hub)
spec = json.load(read_schema)
# Uset the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
hub = ns.Hub
hub_message_structure = []

def enrich_msg_lines_hub(gen_line_with_attribute, zip_line):
    gen_line_with_attribute.Enterprise_Code= str(zip_line[0])
    gen_line_with_attribute.Enterprise_Description = str(zip_line[1])
    gen_line_with_attribute.Site_Name = str(zip_line[2])
    gen_line_with_attribute.Site_Description = str(zip_line[3])
    gen_line_with_attribute.Site_Type = str(zip_line[4])
    hub_message_structure.append(gen_line_with_attribute)
    return hub_message_structure

gen_line_with_attribute_hub = hub()

def read_hub_data():
    global gen_line_with_attribute_hub
    loc_file = open("Hub-1.0", "r")
    loc_file_as_string = loc_file.read()
    d = StringIO(unicode(loc_file_as_string))
    data = numpy.loadtxt(d, dtype='str', delimiter='\t', usecols=(0, 1, 3, 4, 5), unpack=True, skiprows=1)
    # arr = numpy.genfromtxt(d, delimiter=('|'), autostrip=True)
    data_zipped = zip(data[0], data[1], data[2], data[3], data[4])
    for each_value in data_zipped:
        enrich_msg_lines_hub(gen_line_with_attribute_hub, each_value)

count = 0        

def f3(msg_line):
    global count
    logger.console("Verifying {0} exists".format(hub_message_structure[count].Site_Name)) 
    return (msg_line.PLANT_ID == hub_message_structure[count].Site_Name)
def f4(msg_line):
    global count
    logger.console("Verifying {0} exists".format(hub_message_structure[count].Site_Description)) 
    return (msg_line.PLANT_DESC == hub_message_structure[count].Site_Description)

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
    number_of_lines(4)
    logger.console("location master content has {0}".format(msg_structure))
    create_txt_file(msg_structure)
    # wait for the transaction to end
    validate_input_output()
