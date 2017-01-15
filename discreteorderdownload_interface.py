import csv
from robot.api import logger
import pickle
import json
import python_jsonschema_objects as pjs
from io import StringIO
import random, string
import pandas as pd

discreteorder_download = u"""{
	"title": "Discreteorder Download",
	"type": "object",
	"properties": {
		"Order Number": {
 			"type": "string"
		},
		"Order Creation Date": {
 			"type": "string"
		},
		"Order Status": {
 			"type": "string"
		},
		"Order Priority": {
 			"type": "string"
		},
		"Customer ID": {
 			"type": "string"
		},
		"Customer Description": {
 			"type": "string"
		},
		"Customer DUNS": {
 			"type": "string"
		},
		"Customer DUNS+4": {
 			"type": "string"
		},
		"Customer Tax Number": {
 			"type": "string"
		},
		"Customer Address - Descriptor": {
 			"type": "string"
		},
		"Customer Address 1": {
 			"type": "string"
		},
		"Customer Address 2": {
 			"type": "string"
		},
		"Customer Address 3": {
 			"type": "string"
		},
		"Customer Address 4": {
 			"type": "string"
		},
		"Customer Address 5": {
 			"type": "string"
		},
		"Customer City": {
 			"type": "string"
		},
		"Customer County": {
 			"type": "string"
		},
		"Customer State": {
 			"type": "string"
		},
		"Customer Country": {
 			"type": "string"
		},
		"Customer Zip": {
 			"type": "string"
		},
		"Supplier ID": {
 			"type": "string"
		},
		"Supplier Description": {
 			"type": "string"
		},
		"Supplier DUNS": {
 			"type": "string"
		},
		"Supplier DUNS+4": {
 			"type": "string"
		},
		"Supplier Address - Descriptor": {
 			"type": "string"
		},
		"Supplier Address 1": {
 			"type": "string"
		},
		"Supplier Address 2": {
 			"type": "string"
		},
		"Supplier Address 3": {
 			"type": "string"
		},
		"Supplier Address 4": {
 			"type": "string"
		},
		"Supplier Address 5": {
 			"type": "string"
		},
		"Supplier City": {
 			"type": "string"
		},
		"Supplier County": {
 			"type": "string"
		},
		"Supplier State": {
 			"type": "string"
		},
		"Supplier Country": {
 			"type": "string"
		},
		"Supplier Zip": {
 			"type": "string"
		},
		"Buyer Code": {
 			"type": "string"
		},
		"Buyer Contact": {
 			"type": "string"
		},
		"Buyer Name": {
 			"type": "string"
		},
		"Buyer Email": {
 			"type": "string"
		},
		"Supplier Email": {
 			"type": "string"
		},
		"Freight": {
 			"type": "string"
		},
		"Payment Terms": {
 			"type": "string"
		},
		"Total Order Amount": {
 			"type": "string"
		},
		"InCo Terms": {
 			"type": "string"
		},
		"Customer Order Notes": {
 			"type": "string"
		},
		"Supplier Order Notes": {
 			"type": "string"
		},
		"Bill To": {
 			"type": "string"
		},
		"Bill To Address - Descriptor": {
 			"type": "string"
		},
		"Bill To Address 1": {
 			"type": "string"
		},
		"Bill To Address 2": {
 			"type": "string"
		},
		"Bill To Address 3": {
 			"type": "string"
		},
		"Bill To Address 4": {
 			"type": "string"
		},
		"Bill To Address 5": {
 			"type": "string"
		},
		"Bill To City": {
 			"type": "string"
		},
		"Bill To County": {
 			"type": "string"
		},
		"Bill To State": {
 			"type": "string"
		},
		"Bill To Country": {
 			"type": "string"
		},
		"Bill To Zip": {
 			"type": "string"
		},
		"Remit To Address - Descriptor": {
 			"type": "string"
		},
		"Remit To Address 1": {
 			"type": "string"
		},
		"Remit To Address 2": {
 			"type": "string"
		},
		"Remit To Address 3": {
 			"type": "string"
		},
		"Remit To Address 4": {
 			"type": "string"
		},
		"Remit To Address 5": {
 			"type": "string"
		},
		"Remit To City": {
 			"type": "string"
		},
		"Remit To County": {
 			"type": "string"
		},
		"Remit To State": {
 			"type": "string"
		},
		"Remit To Country": {
 			"type": "string"
		},
		"Remit To Zip": {
 			"type": "string"
		},
		"Flex String PO Header 1": {
 			"type": "string"
		},
		"Flex String PO Header 2": {
 			"type": "string"
		},
		"Flex String PO Header 3": {
 			"type": "string"
		},
		"Flex String PO Header 4": {
 			"type": "string"
		},
		"Flex String PO Header 5": {
 			"type": "string"
		},
		"Flex String PO Header 6": {
 			"type": "string"
		},
		"Flex String PO Header 7": {
 			"type": "string"
		},
		"Flex String PO Header 8": {
 			"type": "string"
		},
		"Flex String PO Header 9": {
 			"type": "string"
		},
		"Flex Int PO Header 1": {
 			"type": "string"
		},
		"Flex Int PO Header 2": {
 			"type": "string"
		},
		"Flex Int PO Header 3": {
 			"type": "string"
		},
		"Flex Int PO Header 4": {
 			"type": "string"
		},
		"Flex Int PO Header 5": {
 			"type": "string"
		},
		"Flex Float PO Header 1": {
 			"type": "string"
		},
		"Flex Float PO Header 2": {
 			"type": "string"
		},
		"Flex Float PO Header 3": {
 			"type": "string"
		},
		"Flex Float PO Header 4": {
 			"type": "string"
		},
		"Flex Float PO Header 5": {
 			"type": "string"
		},
		"Flex Date PO Header 1": {
 			"type": "string"
		},
		"Flex Date PO Header 2": {
 			"type": "string"
		},
		"Flex Date PO Header 3": {
 			"type": "string"
		},
		"Flex Date PO Header 4": {
 			"type": "string"
		},
		"Flex Date PO Header 5": {
 			"type": "string"
		},
		"Line ID": {
 			"type": "string"
		},
		"Line Status": {
 			"type": "string"
		},
		"Customer Item ID": {
 			"type": "string"
		},
		"Customer Item Description": {
 			"type": "string"
		},
		"Supplier Item ID": {
 			"type": "string"
		},
		"Supplier Item Description": {
 			"type": "string"
		},
		"Unit Price": {
 			"type": "string"
		},
		"Price Basis": {
 			"type": "string"
		},
		"Currency": {
 			"type": "string"
		},
		"Total Line Amount": {
 			"type": "string"
		},
		"UOM": {
 			"type": "string"
		},
		"Customer Order Line Notes": {
 			"type": "string"
		},
		"Supplier Order Line Notes": {
 			"type": "string"
		},
		"Flex String PO Line 1": {
 			"type": "string"
		},
		"Flex String PO Line 2": {
 			"type": "string"
		},
		"Flex String PO Line 3": {
 			"type": "string"
		},
		"Flex String PO Line 4": {
 			"type": "string"
		},
		"Flex String PO Line 5": {
 			"type": "string"
		},
		"Flex String PO Line 6": {
 			"type": "string"
		},
		"Flex String PO Line 7": {
 			"type": "string"
		},
		"Flex String PO Line 8": {
 			"type": "string"
		},
		"Flex String PO Line 9": {
 			"type": "string"
		},
		"Flex Int PO Line 1": {
 			"type": "string"
		},
		"Flex Int PO Line 2": {
 			"type": "string"
		},
		"Flex Int PO Line 3": {
 			"type": "string"
		},
		"Flex Int PO Line 4": {
 			"type": "string"
		},
		"Flex Int PO Line 5": {
 			"type": "string"
		},
		"Flex Float PO Line 1": {
 			"type": "string"
		},
		"Flex Float PO Line 2": {
 			"type": "string"
		},
		"Flex Float PO Line 3": {
 			"type": "string"
		},
		"Flex Float PO Line 4": {
 			"type": "string"
		},
		"Flex Float PO Line 5": {
 			"type": "string"
		},
		"Flex Date PO Line 1": {
 			"type": "string"
		},
		"Flex Date PO Line 2": {
 			"type": "string"
		},
		"Flex Date PO Line 3": {
 			"type": "string"
		},
		"Flex Date PO Line 4": {
 			"type": "string"
		},
		"Flex Date PO Line 5": {
 			"type": "string"
		},
		"Request ID": {
 			"type": "string"
		},
		"Request Status": {
 			"type": "string"
		},
		"Action": {
 			"type": "string"
		},
		"Request Qty": {
 			"type": "string"
		},
		"Request Date": {
 			"type": "string"
		},
		"Requested Ship Date": {
 			"type": "string"
		},
		"Carrier": {
 			"type": "string"
		},
		"Customer Site": {
 			"type": "string"
		},
		"Ship To Address - Descriptor": {
 			"type": "string"
		},
		"Ship To Address 1": {
 			"type": "string"
		},
		"Ship To Address 2": {
 			"type": "string"
		},
		"Ship To Address 3": {
 			"type": "string"
		},
		"Ship To Address 4": {
 			"type": "string"
		},
		"Ship To Address 5": {
 			"type": "string"
		},
		"Ship To City": {
 			"type": "string"
		},
		"Ship To County": {
 			"type": "string"
		},
		"Ship To State": {
 			"type": "string"
		},
		"Ship To Country": {
 			"type": "string"
		},
		"Ship To Zip": {
 			"type": "string"
		},
		"Ref Order Type": {
 			"type": "string"
		},
		"Ref Order ID": {
 			"type": "string"
		},
		"Ref Order Line ID": {
 			"type": "string"
		},
		"Ref Order Request ID": {
 			"type": "string"
		},
		"Ref Customer ID": {
 			"type": "string"
		},
		"Ref Supplier ID": {
 			"type": "string"
		},
		"Flex String PO Request 1": {
 			"type": "string"
		},
		"Flex String PO Request 2": {
 			"type": "string"
		},
		"Flex String PO Request 3": {
 			"type": "string"
		},
		"Flex String PO Request 4": {
 			"type": "string"
		},
		"Flex String PO Request 5": {
 			"type": "string"
		},
		"Flex String PO Request 6": {
 			"type": "string"
		},
		"Flex String PO Request 7": {
 			"type": "string"
		},
		"Flex String PO Request 8": {
 			"type": "string"
		},
		"Flex String PO Request 9": {
 			"type": "string"
		},
		"Flex Int PO Request 1": {
 			"type": "string"
		},
		"Flex Int PO Request 2": {
 			"type": "string"
		},
		"Flex Int PO Request 3": {
 			"type": "string"
		},
		"Flex Int PO Request 4": {
 			"type": "string"
		},
		"Flex Int PO Request 5": {
 			"type": "string"
		},
		"Flex Float PO Request 1": {
 			"type": "string"
		},
		"Flex Float PO Request 2": {
 			"type": "string"
		},
		"Flex Float PO Request 3": {
 			"type": "string"
		},
		"Flex Float PO Request 4": {
 			"type": "string"
		},
		"Flex Float PO Request 5": {
 			"type": "string"
		},
		"Flex Date PO Request 1": {
 			"type": "string"
		},
		"Flex Date PO Request 2": {
 			"type": "string"
		},
		"Flex Date PO Request 3": {
 			"type": "string"
		},
		"Flex Date PO Request 4": {
 			"type": "string"
		},
		"Flex Date PO Request 5": {
 			"type": "string"
		},
		"Promise ID": {
 			"type": "string"
		},
		"Promise Qty": {
 			"type": "string"
		},
		"Promise Date": {
 			"type": "string"
		},
		"Promised Ship Date": {
 			"type": "string"
		},
		"Supplier Site": {
 			"type": "string"
		},
		"Ship From Address - Descriptor": {
 			"type": "string"
		},
		"Ship From Address 1": {
 			"type": "string"
		},
		"Ship From Address 2": {
 			"type": "string"
		},
		"Ship From Address 3": {
 			"type": "string"
		},
		"Ship From Address 4": {
 			"type": "string"
		},
		"Ship From Address 5": {
 			"type": "string"
		},
		"Ship From City": {
 			"type": "string"
		},
		"Ship From County": {
 			"type": "string"
		},
		"Ship From State": {
 			"type": "string"
		},
		"Ship From Country": {
 			"type": "string"
		},
		"Ship From Zip": {
 			"type": "string"
		},
		"Flex String PO Promise 1": {
 			"type": "string"
		},
		"Flex String PO Promise 2": {
 			"type": "string"
		},
		"Flex String PO Promise 3": {
 			"type": "string"
		},
		"Flex String PO Promise 4": {
 			"type": "string"
		},
		"Flex String PO Promise 5": {
 			"type": "string"
		},
		"Flex String PO Promise 6": {
 			"type": "string"
		},
		"Flex String PO Promise 7": {
 			"type": "string"
		},
		"Flex String PO Promise 8": {
 			"type": "string"
		},
		"Flex String PO Promise 9": {
 			"type": "string"
		},
		"Flex Int PO Promise 1": {
 			"type": "string"
		},
		"Flex Int PO Promise 2": {
 			"type": "string"
		},
		"Flex Int PO Promise 3": {
 			"type": "string"
		},
		"Flex Int PO Promise 4": {
 			"type": "string"
		},
		"Flex Int PO Promise 5": {
 			"type": "string"
		},
		"Flex Float PO Promise 1": {
 			"type": "string"
		},
		"Flex Float PO Promise 2": {
 			"type": "string"
		},
		"Flex Float PO Promise 3": {
 			"type": "string"
		},
		"Flex Float PO Promise 4": {
 			"type": "string"
		},
		"Flex Float PO Promise 5": {
 			"type": "string"
		},
		"Flex Date PO Promise 1": {
 			"type": "string"
		},
		"Flex Date PO Promise 2": {
 			"type": "string"
		},
		"Flex Date PO Promise 3": {
 			"type": "string"
		},
		"Flex Date PO Promise 4": {
 			"type": "string"
		},
		"Flex Date PO Promise 5": {
 			"type": "string"
		},
		"Rev #": {
 			"type": "string"
		},
		"Ship To Customer ID": {
 			"type": "string"
		}
	},
	  "required": ["Order Number"]
}"""

read_schema = StringIO(discreteorder_download)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
DoDownload = ns.DiscreteorderDownload
msg_structure = []
msg_structure_reload = []

Position_of_DODownload_Header_label = ["Order Number", "Order Creation Date", "Order Status", "Order Priority",
                                "Customer ID", "Customer Description", "Customer DUNS", "Customer DUNS+4",
                                 "Customer Tax Number", "Customer Address - Descriptor", "Customer Address 1",
                                 "Customer Address 2", "Customer Address 3", "Customer Address 4", "Customer Address 5",
                                 "Customer City", "Customer County", "Customer State", "Customer Country",
                                 "Customer Zip", "Supplier ID", "Supplier Description", "Supplier DUNS", "Supplier DUNS+4",
                                 "Supplier Address - Descriptor", "Supplier Address 1", "Supplier Address 2",
                                 "Supplier Address 3", "Supplier Address 4", "Supplier Address 5", "Supplier City",
                                 "Supplier County", "Supplier State", "Supplier Country", "Supplier Zip", "Buyer Code",
                                 "Buyer Contact", "Buyer Name", "Buyer Email", "Supplier Email", "Freight",
                                 "Payment Terms", "Total Order Amount", "InCo Terms", "Customer Order Notes",
                                 "Supplier Order Notes", "Bill To", "Bill To Address - Descriptor", "Bill To Address 1",
                                 "Bill To Address 2", "Bill To Address 3", "Bill To Address 4", "Bill To Address 5",
                                 "Bill To City", "Bill To County", "Bill To State", "Bill To Country", "Bill To Zip",
                                 "Remit To Address - Descriptor", "Remit To Address 1", "Remit To Address 2",
                                 "Remit To Address 3", "Remit To Address 4", "Remit To Address 5", "Remit To City",
                                 "Remit To County", "Remit To State", "Remit To Country", "Remit To Zip",
                                 "Flex String PO Header 1", "Flex String PO Header 2", "Flex String PO Header 3",
                                 "Flex String PO Header 4", "Flex String PO Header 5", "Flex String PO Header 6",
                                 "Flex String PO Header 7", "Flex String PO Header 8", "Flex String PO Header 9",
                                 "Flex Int PO Header 1", "Flex Int PO Header 2", "Flex Int PO Header 3",
                                 "Flex Int PO Header 4", "Flex Int PO Header 5", "Flex Float PO Header 1",
                                 "Flex Float PO Header 2", "Flex Float PO Header 3", "Flex Float PO Header 4",
                                 "Flex Float PO Header 5", "Flex Date PO Header 1", "Flex Date PO Header 2",
                                 "Flex Date PO Header 3", "Flex Date PO Header 4", "Flex Date PO Header 5", "Line ID",
                                 "Line Status", "Customer Item ID", "Customer Item Description", "Supplier Item ID",
                                 "Supplier Item Description", "Unit Price", "Price Basis", "Currency",
                                 "Total Line Amount", "UOM", "Customer Order Line Notes", "Supplier Order Line Notes",
                                 "Flex String PO Line 1", "Flex String PO Line 2", "Flex String PO Line 3",
                                 "Flex String PO Line 4", "Flex String PO Line 5", "Flex String PO Line 6",
                                 "Flex String PO Line 7", "Flex String PO Line 8", "Flex String PO Line 9",
                                 "Flex Int PO Line 1", "Flex Int PO Line 2", "Flex Int PO Line 3", "Flex Int PO Line 4",
                                 "Flex Int PO Line 5", "Flex Float PO Line 1", "Flex Float PO Line 2",
                                 "Flex Float PO Line 3", "Flex Float PO Line 4", "Flex Float PO Line 5",
                                 "Flex Date PO Line 1", "Flex Date PO Line 2", "Flex Date PO Line 3",
                                 "Flex Date PO Line 4", "Flex Date PO Line 5", "Request ID", "Request Status", "Action",
                                 "Request Qty", "Request Date", "Requested Ship Date", "Carrier", "Customer Site",
                                 "Ship To Address - Descriptor", "Ship To Address 1", "Ship To Address 2",
                                 "Ship To Address 3", "Ship To Address 4", "Ship To Address 5", "Ship To City",
                                 "Ship To County", "Ship To State", "Ship To Country", "Ship To Zip", "Ref Order Type",
                                 "Ref Order ID", "Ref Order Line ID", "Ref Order Request ID", "Ref Customer ID",
                                 "Ref Supplier ID", "Flex String PO Request 1", "Flex String PO Request 2",
                                 "Flex String PO Request 3", "Flex String PO Request 4", "Flex String PO Request 5",
                                 "Flex String PO Request 6", "Flex String PO Request 7", "Flex String PO Request 8",
                                 "Flex String PO Request 9", "Flex Int PO Request 1", "Flex Int PO Request 2",
                                 "Flex Int PO Request 3", "Flex Int PO Request 4", "Flex Int PO Request 5",
                                 "Flex Float PO Request 1", "Flex Float PO Request 2", "Flex Float PO Request 3",
                                 "Flex Float PO Request 4", "Flex Float PO Request 5", "Flex Date PO Request 1",
                                 "Flex Date PO Request 2", "Flex Date PO Request 3", "Flex Date PO Request 4",
                                 "Flex Date PO Request 5", "Promise ID", "Promise Qty", "Promise Date",
                                 "Promised Ship Date", "Supplier Site", "Ship From Address - Descriptor",
                                 "Ship From Address 1", "Ship From Address 2", "Ship From Address 3",
                                 "Ship From Address 4", "Ship From Address 5", "Ship From City", "Ship From County",
                                 "Ship From State", "Ship From Country", "Ship From Zip", "Flex String PO Promise 1",
                                 "Flex String PO Promise 2", "Flex String PO Promise 3", "Flex String PO Promise 4",
                                 "Flex String PO Promise 5", "Flex String PO Promise 6", "Flex String PO Promise 7",
                                 "Flex String PO Promise 8", "Flex String PO Promise 9", "Flex Int PO Promise 1",
                                 "Flex Int PO Promise 2", "Flex Int PO Promise 3", "Flex Int PO Promise 4",
                                 "Flex Int PO Promise 5", "Flex Float PO Promise 1", "Flex Float PO Promise 2",
                                 "Flex Float PO Promise 3", "Flex Float PO Promise 4", "Flex Float PO Promise 5",
                                 "Flex Date PO Promise 1", "Flex Date PO Promise 2", "Flex Date PO Promise 3",
                                 "Flex Date PO Promise 4", "Flex Date PO Promise 5", "Rev #", "Ship To Customer ID"]

def random_int(length):
    valid_letters = '1234567890'
    return ''.join((random.choice(valid_letters) for i in xrange(length)))

def random_word(length):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def random_pick(choices):
    return random.choice(choices)

# create message lines with these attributes
def enrich_msg_lines(gen_line_with_attribute):
    #gen_line_with_attribute["Order Number 1"] = "2"
    gen_line_with_attribute["Order Number"] = random_pick(['0745000', '0745000', '0745000', '07450']) + random_int(6)
    gen_line_with_attribute["Order Creation Date"] = random_pick(["2017-01-05T00:00:00+0000","2017-01-05T00:00:00+0000"])
    gen_line_with_attribute["Customer Address - Descriptor"] = "provide address description here"
    gen_line_with_attribute["Promise Qty"] = random_int(3)
    return gen_line_with_attribute

def msgline_to_list(msg_line):
    global msg_structure
    logger.console("Adding msg line to msg_structure")
    msg_structure.append(msg_line)
    create_txt_file(msg_structure)

def create_txt_file(msg_structure, file_name='DiscreteOrderDownload.TXT'):
    with open('DiscreteOrderDownload.TXT-UNORDERED', 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter="\t")
        wr.writerow(msg_structure[0].keys())
        for msg_line in msg_structure:
            wr.writerow(msg_line.values())

    with open('DiscreteOrderDownload.TXT-UNORDERED', 'rb') as input_file:
        with open(file_name, 'wb') as output_file:
            read_csv = csv.DictReader(input_file, delimiter='\t')
            # save ignoring certain columns by using list: position_of_header or position_of_header_subset
            write_csv = csv.DictWriter(output_file, Position_of_DODownload_Header_label, delimiter='\t', extrasaction='ignore')
            wr = csv.writer(output_file, delimiter="\t")
            wr.writerow(Position_of_DODownload_Header_label)
            for read_row in read_csv:
                write_csv.writerow(read_row)

def serialize_msg_structure():
    with open('DiscreteOrderDownload.ser', 'wb') as f:
        # order of msg_structure is changed as it is sorted in place
        for each_msg_line in msg_structure:
            pickle.dump(each_msg_line.as_dict(), f)

# messageIO
# load from file
def deserialize_msg_structure():
    objects = []
    global msg_structure_reload;
    with open('DiscreteOrderDownload.ser', 'rb') as openfile:
        while True:
            try:
                objects.append(pickle.load(openfile))
            except EOFError:
                break

    for each_obj in objects:
        msg_line_as_json = json.dumps(each_obj)
        create_line_on_deserialization = DoDownload.from_json(msg_line_as_json)
        msg_structure_reload.append(create_line_on_deserialization)

def create_zip_file():
    pass

def number_of_lines(lines_to_generate=1):
    for each_line in range(lines_to_generate):
        gen_line_with_attribute = DoDownload()
        gen_line_with_attribute = enrich_msg_lines(gen_line_with_attribute)
        msgline_to_list(gen_line_with_attribute)
        serialize_msg_structure()
        create_zip_file()

poack = u"""{
    "title": "SAP PurcahseOrderAck",
    "type": "object",
    "properties": {
        "ORDER_ID": {
            "position": "0",
            "type": "string"
        },
        "CANCELLED_FLAG": {
            "position": "1",
            "type": "string"
        },
        "ITEM_NO": {
            "position": "2",
            "type": "string"
            },
        "DELIVERY_CANCELLED_FLAG": {
            "position": "3",
            "type": "string"
            },
        "DELIVERY_DATETIME": {
            "position": "4",
            "type": "string"
        },
        "DELIVERY_DATE_TYPE": {
            "position": "5",
            "type": "string"
        },
        "QUANTITY": {
            "position": "6",
            "type": "string"
        }
    }
}"""

read_schema = StringIO(poack)
spec = json.load(read_schema)
# Uset the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
poack = ns.SapPurcahseorderack
poack_message_structure = []

'''ORDER_ID
CANCELLED_FLAG
CONFIRMED_AS_ORDERED
ITEM_NO
DELIVERY_CANCELLED_FLAG
DELIVERY_DATETIME
DELIVERY_DATE_TYPE
QUANTITY
'''

def enrich_msg_lines_hub(gen_line_with_attribute, zip_line):
    gen_line_with_attribute["ORDER_ID"] = zip_line[0]
    gen_line_with_attribute["CANCELLED_FLAG"] = zip_line[1]
    gen_line_with_attribute["CONFIRMED_AS_ORDERED"] = zip_line[2]
    gen_line_with_attribute["DELIVERY_CANCELLED_FLAG"] = zip_line[3]
    gen_line_with_attribute["DELIVERY_DATETIME"] = zip_line[4]
    gen_line_with_attribute["DELIVERY_DATE_TYPE"] = zip_line[5]
    gen_line_with_attribute["QUANTITY"] = zip_line[6]
    poack_message_structure.append(gen_line_with_attribute)
    return poack_message_structure

gen_line_with_attribute_poack = poack()

def read_poack_data():
    global gen_line_with_attribute_poack
    loc_file = open("Hub-1.0", "r")
    logger.console ("reading file Hub-1.0")
    loc_file_as_string = loc_file.read()
    logger.console ("reding file ..")
    d = StringIO(unicode(loc_file_as_string))
    logger.console ("table as unicode string is {}".format(d))
    fields = gen_line_with_attribute_poack.keys()
    df = pd.read_csv(d, usecols=fields, delimiter='\t', dtype=str)
    for index, each_row in df.iterrows():
        data_zipped = []
        data_zipped.append(each_row[["ORDER_ID"]])
        data_zipped.append(each_row["CANCELLED_FLAG"])
        data_zipped.append(each_row["CONFIRMED_AS_ORDERED"])
        data_zipped.append(each_row["DELIVERY_CANCELLED_FLAG"])
        data_zipped.append(each_row["DELIVERY_DATETIME"])
        data_zipped.append(each_row["DELIVERY_DATE_TYPE"])
        data_zipped.append(each_row["QUANTITY"])
        gen_line_with_attribute_hub = poack()
        enrich_msg_lines_hub(gen_line_with_attribute_hub, data_zipped)

count = 0

def f3(msg_line):
    global count
    return (msg_line["Order Number"] == poack_message_structure[count]["ORDER_ID"])

def f4(msg_line):
    global count
    return (msg_line["Promise Qty"] == poack_message_structure[count]["QUANTITY"])

def simple_filter(filters, msg_structure):
    for f in filters:
        msg_structure = filter(f, msg_structure)
        if not msg_structure:
            return msg_structure
    return msg_structure

def validate_input_output():
    deserialize_msg_structure()
    read_poack_data()
    filtered_rows = []
    global msg_structure_reload
    logger.console ("length of hub msg lines to verify {}".format(len(poack_message_structure)))
    for i in range(0,len(poack_message_structure)-1):
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