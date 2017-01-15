import csv
from robot.api import logger
import pickle
import json
import python_jsonschema_objects as pjs
from io import StringIO
import random, string
import pandas as pd

shipment_download = u"""{
  "title": "Shipment SSP",
  "type": "object",
  "properties":
{
  "Shipment ID":{
 			"type": "string"
		},
  "Customer ID":{
 			"type": "string"
		},
  "Customer Description":{
 			"type": "string"
		},
  "Customer DUNS":{
 			"type": "string"
		},
  "Customer DUNS+4":{
 			"type": "string"
		},
  "Customer Address - Descriptor":{
 			"type": "string"
		},
  "Customer Address 1":{
 			"type": "string"
		},
  "Customer Address 2":{
 			"type": "string"
		},
  "Customer Address 3":{
 			"type": "string"
		},
  "Customer Address 4":{
 			"type": "string"
		},
  "Customer Address 5":{
 			"type": "string"
		},
  "Customer City":{
 			"type": "string"
		},
  "Customer County":{
 			"type": "string"
		},
  "Customer State":{
 			"type": "string"
		},
  "Customer Country":{
 			"type": "string"
		},
  "Customer Zip":{
 			"type": "string"
		},
  "Supplier ID":{
 			"type": "string"
		},
  "Supplier Description":{
 			"type": "string"
		},
  "Supplier DUNS":{
 			"type": "string"
		},
  "Supplier DUNS+4":{
 			"type": "string"
		},
  "Supplier Address - Descriptor":{
 			"type": "string"
		},
  "Supplier Address 1":{
 			"type": "string"
		},
  "Supplier Address 2":{
 			"type": "string"
		},
  "Supplier Address 3":{
 			"type": "string"
		},
  "Supplier Address 4":{
 			"type": "string"
		},
  "Supplier Address 5":{
 			"type": "string"
		},
  "Supplier City":{
 			"type": "string"
		},
  "Supplier County":{
 			"type": "string"
		},
  "Supplier State":{
 			"type": "string"
		},
  "Supplier Country":{
 			"type": "string"
		},
  "Supplier Zip":{
 			"type": "string"
		},
  "Shipment Creation Date":{
 			"type": "string"
		},
  "Shipment Creator Code":{
 			"type": "string"
		},
  "Shipment Status":{
 			"type": "string"
		},
  "Shipment Date":{
 			"type": "string"
		},
  "Planned Delivery Date":{
 			"type": "string"
		},
  "Final Site ETA":{
 			"type": "string"
		},
  "Last Known Milestone":{
 			"type": "string"
		},
  "Last Known Milestone Arrival":{
 			"type": "string"
		},
  "Last Known Milestone Departure":{
 			"type": "string"
		},
  "Carrier":{
 			"type": "string"
		},
  "Shipment Mode":{
 			"type": "string"
		},
  "Waybill #":{
 			"type": "string"
		},
  "Shipment Delivery BOL #":{
 			"type": "string"
		},
  "Packing Slip #":{
 			"type": "string"
		},
  "Carrier Reference #":{
 			"type": "string"
		},
  "Shipper Reference #":{
 			"type": "string"
		},
  "Ship To Type (Site/Hub)":{
 			"type": "string"
		},
  "Country Of Origin":{
 			"type": "string"
		},
  "Number Of Packages":{
 			"type": "string"
		},
  "License Plate":{
 			"type": "string"
		},
  "Importer Of #":{
 			"type": "string"
		},
  "Consignee #":{
 			"type": "string"
		},
  "Foreign Port Of Unlading":{
 			"type": "string"
		},
  "Place Of Delivery":{
 			"type": "string"
		},
  "Ship To Site":{
 			"type": "string"
		},
  "Ship-to-Party Name":{
 			"type": "string"
		},
  "Ship-to-Party DUNS":{
 			"type": "string"
		},
  "Ship To Address - Descriptor":{
 			"type": "string"
		},
  "Ship To Address 1":{
 			"type": "string"
		},
  "Ship To Address 2":{
 			"type": "string"
		},
  "Ship To Address 3":{
 			"type": "string"
		},
  "Ship To Address 4":{
 			"type": "string"
		},
  "Ship To Address 5":{
 			"type": "string"
		},
  "Ship To City":{
 			"type": "string"
		},
  "Ship To County":{
 			"type": "string"
		},
  "Ship To State":{
 			"type": "string"
		},
  "Ship To Country":{
 			"type": "string"
		},
  "Ship To Zip":{
 			"type": "string"
		},
  "Supplier Site":{
 			"type": "string"
		},
  "Ship From Address - Descriptor":{
 			"type": "string"
		},
  "Ship From Address 1":{
 			"type": "string"
		},
  "Ship From Address 2":{
 			"type": "string"
		},
  "Ship From Address 3":{
 			"type": "string"
		},
  "Ship From Address 4":{
 			"type": "string"
		},
  "Ship From Address 5":{
 			"type": "string"
		},
  "Ship From City":{
 			"type": "string"
		},
  "Ship From County":{
 			"type": "string"
		},
  "Ship From State":{
 			"type": "string"
		},
  "Ship From Country":{
 			"type": "string"
		},
  "Ship From Zip":{
 			"type": "string"
		},
  "Bill To":{
 			"type": "string"
		},
  "Bill To Address - Descriptor":{
 			"type": "string"
		},
  "Bill To Address 1":{
 			"type": "string"
		},
  "Bill To Address 2":{
 			"type": "string"
		},
  "Bill To Address 3":{
 			"type": "string"
		},
  "Bill To Address 4":{
 			"type": "string"
		},
  "Bill To Address 5":{
 			"type": "string"
		},
  "Bill To City":{
 			"type": "string"
		},
  "Bill To County":{
 			"type": "string"
		},
  "Bill To State":{
 			"type": "string"
		},
  "Bill To Country":{
 			"type": "string"
		},
  "Bill To Zip":{
 			"type": "string"
		},
  "Booking Party Name":{
 			"type": "string"
		},
  "Booking Party DUNS":{
 			"type": "string"
		},
  "Booking Party Address - Descriptor":{
 			"type": "string"
		},
  "Booking Party Address 1":{
 			"type": "string"
		},
  "Booking Party Address 2":{
 			"type": "string"
		},
  "Booking Party Address 3":{
 			"type": "string"
		},
  "Booking Party Address 4":{
 			"type": "string"
		},
  "Booking Party Address 5":{
 			"type": "string"
		},
  "Booking Party City":{
 			"type": "string"
		},
  "Booking Party County":{
 			"type": "string"
		},
  "Booking Party State":{
 			"type": "string"
		},
  "Booking Party Country":{
 			"type": "string"
		},
  "Booking Party  Zip":{
 			"type": "string"
		},
  "Flex String ASN Header 1":{
 			"type": "string"
		},
  "Flex String ASN Header 2":{
 			"type": "string"
		},
  "Flex String ASN Header 3":{
 			"type": "string"
		},
  "Flex String ASN Header 4":{
 			"type": "string"
		},
  "Flex String ASN Header 5":{
 			"type": "string"
		},
  "Flex String ASN Header 6":{
 			"type": "string"
		},
  "Flex String ASN Header 7":{
 			"type": "string"
		},
  "Flex String ASN Header 8":{
 			"type": "string"
		},
  "Flex String ASN Header 9":{
 			"type": "string"
		},
  "Flex Int ASN Header 1":{
 			"type": "string"
		},
  "Flex Int ASN Header 2":{
 			"type": "string"
		},
  "Flex Int ASN Header 3":{
 			"type": "string"
		},
  "Flex Int ASN Header 4":{
 			"type": "string"
		},
  "Flex Int ASN Header 5":{
 			"type": "string"
		},
  "Flex Float ASN Header 1":{
 			"type": "string"
		},
  "Flex Float ASN Header 2":{
 			"type": "string"
		},
  "Flex Float ASN Header 3":{
 			"type": "string"
		},
  "Flex Float ASN Header 4":{
 			"type": "string"
		},
  "Flex Float ASN Header 5":{
 			"type": "string"
		},
  "Flex Date ASN Header 1":{
 			"type": "string"
		},
  "Flex Date ASN Header 2":{
 			"type": "string"
		},
  "Flex Date ASN Header 3":{
 			"type": "string"
		},
  "Flex Date ASN Header 4":{
 			"type": "string"
		},
  "Flex Date ASN Header 5":{
 			"type": "string"
		},
  "Shipment Line ID":{
 			"type": "string"
		},
  "Shipment Line Status":{
 			"type": "string"
		},
  "Action":{
 			"type": "string"
		},
  "Customer Item ID":{
 			"type": "string"
		},
  "Customer Item Description":{
 			"type": "string"
		},
  "Supplier Item ID":{
 			"type": "string"
		},
  "Supplier Item Description":{
 			"type": "string"
		},
  "Buyer Code":{
 			"type": "string"
		},
  "Shipped Quantity":{
 			"type": "string"
		},
  "Unit Of Measure":{
 			"type": "string"
		},
  "Freight Cost":{
 			"type": "string"
		},
  "Currency":{
 			"type": "string"
		},
  "Packing Slip # (Line)":{
 			"type": "string"
		},
  "Notes To Customer":{
 			"type": "string"
		},
  "# Of Boxes":{
 			"type": "string"
		},
  "Weight":{
 			"type": "string"
		},
  "Commodity HTS-6":{
 			"type": "string"
		},
  "Manufacturer Name":{
 			"type": "string"
		},
  "Manufacturer DUNS":{
 			"type": "string"
		},
  "Manufacturer Address - Descriptor":{
 			"type": "string"
		},
  "Manufacturer Address 1":{
 			"type": "string"
		},
  "Manufacturer Address 2":{
 			"type": "string"
		},
  "Manufacturer Address 3":{
 			"type": "string"
		},
  "Manufacturer Address 4":{
 			"type": "string"
		},
  "Manufacturer Address 5":{
 			"type": "string"
		},
  "Manufacturer City":{
 			"type": "string"
		},
  "Manufacturer County":{
 			"type": "string"
		},
  "Manufacturer State":{
 			"type": "string"
		},
  "Manufacturer Country":{
 			"type": "string"
		},
  "Manufacturer Zip":{
 			"type": "string"
		},
  "Container Stuffing Location Name":{
 			"type": "string"
		},
  "Container Stuffing Location DUNS":{
 			"type": "string"
		},
  "Container Stuffing Location Address - Descriptor":{
 			"type": "string"
		},
  "Container Stuffing Location Address 1":{
 			"type": "string"
		},
  "Container Stuffing Location Address 2":{
 			"type": "string"
		},
  "Container Stuffing Location Address 3":{
 			"type": "string"
		},
  "Container Stuffing Location Address 4":{
 			"type": "string"
		},
  "Container Stuffing Location Address 5":{
 			"type": "string"
		},
  "Container Stuffing Location City":{
 			"type": "string"
		},
  "Container Stuffing Location County":{
 			"type": "string"
		},
  "Container Stuffing Location State":{
 			"type": "string"
		},
  "Container Stuffing Location Country":{
 			"type": "string"
		},
  "Container Stuffing Location  Zip":{
 			"type": "string"
		},
  "Consolidator Name":{
 			"type": "string"
		},
  "Consolidator DUNS":{
 			"type": "string"
		},
  "Consolidator Address - Descriptor":{
 			"type": "string"
		},
  "Consolidator Address 1":{
 			"type": "string"
		},
  "Consolidator Address 2":{
 			"type": "string"
		},
  "Consolidator Address 3":{
 			"type": "string"
		},
  "Consolidator Address 4":{
 			"type": "string"
		},
  "Consolidator Address 5":{
 			"type": "string"
		},
  "Consolidator City":{
 			"type": "string"
		},
  "Consolidator County":{
 			"type": "string"
		},
  "Consolidator State":{
 			"type": "string"
		},
  "Consolidator Country":{
 			"type": "string"
		},
  "Consolidator Zip":{
 			"type": "string"
		},
  "Ref Customer ID":{
 			"type": "string"
		},
  "Ref Supplier ID":{
 			"type": "string"
		},
  "Ref Order Type":{
 			"type": "string"
		},
  "Ref Order ID":{
 			"type": "string"
		},
  "Ref Order Line ID":{
 			"type": "string"
		},
  "Ref Order Request ID":{
 			"type": "string"
		},
  "Ref Order Promise ID":{
 			"type": "string"
		},
  "Flex String ASN Line 1":{
 			"type": "string"
		},
  "Flex String ASN Line 2":{
 			"type": "string"
		},
  "Flex String ASN Line 3":{
 			"type": "string"
		},
  "Flex String ASN Line 4":{
 			"type": "string"
		},
  "Flex String ASN Line 5":{
 			"type": "string"
		},
  "Flex String ASN Line 6":{
 			"type": "string"
		},
  "Flex String ASN Line 7":{
 			"type": "string"
		},
  "Flex String ASN Line 8":{
 			"type": "string"
		},
  "Flex String ASN Line 9":{
 			"type": "string"
		},
  "Flex Int ASN Line 1":{
 			"type": "string"
		},
  "Flex Int ASN Line 2":{
 			"type": "string"
		},
  "Flex Int ASN Line 3":{
 			"type": "string"
		},
  "Flex Int ASN Line 4":{
 			"type": "string"
		},
  "Flex Int ASN Line 5":{
 			"type": "string"
		},
  "Flex Float ASN Line 1":{
 			"type": "string"
		},
  "Flex Float ASN Line 2":{
 			"type": "string"
		},
  "Flex Float ASN Line 3":{
 			"type": "string"
		},
  "Flex Float ASN Line 4":{
 			"type": "string"
		},
  "Flex Float ASN Line 5":{
 			"type": "string"
		},
  "Flex Date ASN Line 1":{
 			"type": "string"
		},
  "Flex Date ASN Line 2":{
 			"type": "string"
		},
  "Flex Date ASN Line 3":{
 			"type": "string"
		},
  "Flex Date ASN Line 4":{
 			"type": "string"
		},
  "Flex Date ASN Line 5":{
 			"type": "string"
		},
  "Ship To Customer ID":{
 			"type": "string"
		},
  "Ship To Customer Desc":{
 			"type": "string"
		},
  "Ship To Customer Item ID":{
 			"type": "string"
		},
  "Ship To Customer Item Desc":{
 			"type": "string"
		},
  "Drop Shipment Flag":{
 			"type": "string"
		}
}
}"""

read_schema = StringIO(shipment_download)
spec = json.load(read_schema)
# Use the above json spec to build the template class and corresponding object
builder = pjs.ObjectBuilder(spec)
ns = builder.build_classes()
ShipmentDownload = ns.ShipmentSsp
msg_structure = []
msg_structure_reload = []

# to-do:
# replace the list below by extracting the list from json-schema
Position_of_ShipmentDownload_Header_label = ["Shipment ID","Customer ID","Customer Description","Customer DUNS","Customer DUNS+4","Customer Address - Descriptor","Customer Address 1",
"Customer Address 2","Customer Address 3","Customer Address 4","Customer Address 5","Customer City","Customer County","Customer State","Customer Country",
"Customer Zip","Supplier ID","Supplier Description","Supplier DUNS","Supplier DUNS+4","Supplier Address - Descriptor","Supplier Address 1","Supplier Address 2",
"Supplier Address 3","Supplier Address 4","Supplier Address 5","Supplier City","Supplier County","Supplier State","Supplier Country","Supplier Zip","Shipment Creation Date",
"Shipment Creator Code","Shipment Status","Shipment Date","Planned Delivery Date","Final Site ETA","Last Known Milestone","Last Known Milestone Arrival",
"Last Known Milestone Departure","Carrier","Shipment Mode","Waybill #","Shipment Delivery BOL #","Packing Slip #","Carrier Reference #","Shipper Reference #","Ship To Type (Site/Hub)",
"Country Of Origin","Number Of Packages","License Plate","Importer Of #","Consignee #","Foreign Port Of Unlading","Place Of Delivery","Ship To Site","Ship-to-Party Name","Ship-to-Party DUNS",
"Ship To Address - Descriptor","Ship To Address 1","Ship To Address 2","Ship To Address 3","Ship To Address 4","Ship To Address 5","Ship To City","Ship To County","Ship To State",
"Ship To Country","Ship To Zip","Supplier Site","Ship From Address - Descriptor","Ship From Address 1","Ship From Address 2","Ship From Address 3","Ship From Address 4",
"Ship From Address 5","Ship From City","Ship From County","Ship From State","Ship From Country","Ship From Zip","Bill To","Bill To Address - Descriptor","Bill To Address 1",
"Bill To Address 2","Bill To Address 3","Bill To Address 4","Bill To Address 5","Bill To City","Bill To County","Bill To State","Bill To Country","Bill To Zip","Booking Party Name",
"Booking Party DUNS","Booking Party Address - Descriptor","Booking Party Address 1","Booking Party Address 2","Booking Party Address 3","Booking Party Address 4","Booking Party Address 5",
"Booking Party City","Booking Party County","Booking Party State","Booking Party Country","Booking Party  Zip","Flex String ASN Header 1","Flex String ASN Header 2","Flex String ASN Header 3",
"Flex String ASN Header 4","Flex String ASN Header 5","Flex String ASN Header 6","Flex String ASN Header 7","Flex String ASN Header 8","Flex String ASN Header 9","Flex Int ASN Header 1",
"Flex Int ASN Header 2","Flex Int ASN Header 3","Flex Int ASN Header 4","Flex Int ASN Header 5","Flex Float ASN Header 1","Flex Float ASN Header 2","Flex Float ASN Header 3",
"Flex Float ASN Header 4","Flex Float ASN Header 5","Flex Date ASN Header 1","Flex Date ASN Header 2","Flex Date ASN Header 3","Flex Date ASN Header 4","Flex Date ASN Header 5",
"Shipment Line ID","Shipment Line Status","Action","Customer Item ID","Customer Item Description","Supplier Item ID","Supplier Item Description","Buyer Code","Shipped Quantity",
"Unit Of Measure","Freight Cost","Currency","Packing Slip # (Line)","Notes To Customer","# Of Boxes","Weight","Commodity HTS-6","Manufacturer Name","Manufacturer DUNS",
"Manufacturer Address - Descriptor","Manufacturer Address 1","Manufacturer Address 2","Manufacturer Address 3","Manufacturer Address 4","Manufacturer Address 5","Manufacturer City",
"Manufacturer County","Manufacturer State","Manufacturer Country","Manufacturer Zip","Container Stuffing Location Name","Container Stuffing Location DUNS",
"Container Stuffing Location Address - Descriptor","Container Stuffing Location Address 1","Container Stuffing Location Address 2","Container Stuffing Location Address 3",
"Container Stuffing Location Address 4","Container Stuffing Location Address 5","Container Stuffing Location City","Container Stuffing Location County","Container Stuffing Location State",
"Container Stuffing Location Country","Container Stuffing Location  Zip","Consolidator Name","Consolidator DUNS","Consolidator Address - Descriptor","Consolidator Address 1","Consolidator Address 2",
"Consolidator Address 3","Consolidator Address 4","Consolidator Address 5","Consolidator City","Consolidator County","Consolidator State","Consolidator Country","Consolidator Zip","Ref Customer ID",
"Ref Supplier ID","Ref Order Type","Ref Order ID","Ref Order Line ID","Ref Order Request ID","Ref Order Promise ID","Flex String ASN Line 1","Flex String ASN Line 2","Flex String ASN Line 3",
"Flex String ASN Line 4","Flex String ASN Line 5","Flex String ASN Line 6","Flex String ASN Line 7","Flex String ASN Line 8","Flex String ASN Line 9","Flex Int ASN Line 1","Flex Int ASN Line 2",
"Flex Int ASN Line 3","Flex Int ASN Line 4","Flex Int ASN Line 5","Flex Float ASN Line 1","Flex Float ASN Line 2","Flex Float ASN Line 3","Flex Float ASN Line 4","Flex Float ASN Line 5",
"Flex Date ASN Line 1","Flex Date ASN Line 2","Flex Date ASN Line 3","Flex Date ASN Line 4","Flex Date ASN Line 5","Ship To Customer ID","Ship To Customer Desc","Ship To Customer Item ID",
"Ship To Customer Item Desc","Drop Shipment Flag"]

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
    gen_line_with_attribute["Shipment ID"] = random_pick(['0745000', '0745000', '0745000', '07450']) + random_int(6)
    gen_line_with_attribute["Customer ID"] = random_pick(["2017-01-05T00:00:00+0000","2017-01-05T00:00:00+0000"])
    gen_line_with_attribute["Customer Description"] = "provide address description here"
    gen_line_with_attribute["Supplier ID"] = "Supplier test 4"
    gen_line_with_attribute["Supplier Description"] = "0000001000"
    gen_line_with_attribute["Shipment Creation Date"] = "2017-01-09T15:43:49+0000"
    gen_line_with_attribute["Shipment Creator Code"] = "e2open_super_user"
    gen_line_with_attribute["Shipment Status"] = "Shipped"
    gen_line_with_attribute["Shipment Date"] = "2017-01-09T15:43:49+0000"
    gen_line_with_attribute["Planned Delivery Date"] = "2017-01-09T15:43:49+0000"
    gen_line_with_attribute["Ship To Type (Site/Hub)"] = "Site"
    gen_line_with_attribute["Number Of Packages"] = "0"
    gen_line_with_attribute["Ship To Site"] = random_pick(["1000", "2000"])
    gen_line_with_attribute["Flex Int ASN Header 1"] = "0"
    gen_line_with_attribute["Flex Int ASN Header 2"] = "0"
    gen_line_with_attribute["Flex Int ASN Header 3"] = "0"
    gen_line_with_attribute["Flex Int ASN Header 4"] = "0"
    gen_line_with_attribute["Flex Int ASN Header 5"] = "0"
    gen_line_with_attribute["Flex Float ASN Header 1"] = "0.0000"
    gen_line_with_attribute["Flex Float ASN Header 2"] = "0.0000"
    gen_line_with_attribute["Flex Float ASN Header 3"] = "0.0000"
    gen_line_with_attribute["Flex Float ASN Header 4"] = "0.0000"
    gen_line_with_attribute["Flex Float ASN Header 5"] = "0.0000"
    gen_line_with_attribute["Shipment Line ID"] = "1"
    gen_line_with_attribute["Shipment Line Status"] = "Shipped"
    gen_line_with_attribute["Customer Item ID"] = random_pick(["1400-750"])
    gen_line_with_attribute["Customer Item Description"] = "Provide customer item Description"
    gen_line_with_attribute["Supplier Item ID"] = random_pick(["VENDOR_MATERIAL_1", "VENDOR_MATERIAL_2", "VENDOR_MATERIAL_3"])
    gen_line_with_attribute["Shipped Quantity"] = "33.0000"
    gen_line_with_attribute["Unit Of Measure"] = "ST"
    gen_line_with_attribute["Freight Cost"] = "0.0000"
    gen_line_with_attribute["Currency"] = random_pick(["EUR", "USD"])
    gen_line_with_attribute["# Of Boxes"] = "0"
    gen_line_with_attribute["Weight"] = "0.0000"
    gen_line_with_attribute["Ref Order Type"] = random_pick(["DiscreteOrder"])
    gen_line_with_attribute["Ref Order ID"] = random_pick(["074500017594", "074500017595", "074500017596"])
    gen_line_with_attribute["Ref Order Line ID"] = random_pick(["10","30"])
    gen_line_with_attribute["Ref Order Request ID"] = random_pick(['1', '2'])
    gen_line_with_attribute["Ref Order Promise ID"] = "1"
    gen_line_with_attribute["Flex Int ASN Line 1"] = "0"
    gen_line_with_attribute["Flex Int ASN Line 2"] = "0"
    gen_line_with_attribute["Flex Int ASN Line 3"] = "0"
    gen_line_with_attribute["Flex Int ASN Line 4"] = "0"
    gen_line_with_attribute["Flex Int ASN Line 5"] = "0"
    gen_line_with_attribute["Flex Float ASN Line 1"] = "0.0000"
    gen_line_with_attribute["Flex Float ASN Line 2"] = "0.0000"
    gen_line_with_attribute["Flex Float ASN Line 3"] = "0.0000"
    gen_line_with_attribute["Flex Float ASN Line 4"] = "0.0000"
    gen_line_with_attribute["Flex Float ASN Line 5"] = "0.0000"
    gen_line_with_attribute["Ship To Customer ID"] = "1000"
    gen_line_with_attribute["Ship To Customer Desc"]
    gen_line_with_attribute["Ship To Customer Item ID"] = random_pick(["100-100", "1400-750"])
    gen_line_with_attribute["Ship To Customer Item Desc"] = "Casing"
    return gen_line_with_attribute

def msgline_to_list(msg_line):
    global msg_structure
    logger.console("Adding msg line to msg_structure")
    msg_structure.append(msg_line)
    create_txt_file(msg_structure)

def create_txt_file(msg_structure, file_name='ShipmentDownload.file'):
    with open('ShipmentDownload.file-UNORDERED', 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter="\t")
        wr.writerow(msg_structure[0].keys())
        for msg_line in msg_structure:
            wr.writerow(msg_line.values())

    with open('ShipmentDownload.file-UNORDERED', 'rb') as input_file:
        with open(file_name, 'wb') as output_file:
            read_csv = csv.DictReader(input_file, delimiter='\t')
            # save ignoring certain columns by using list: position_of_header or position_of_header_subset
            write_csv = csv.DictWriter(output_file, Position_of_ShipmentDownload_Header_label, delimiter='\t', extrasaction='ignore')
            wr = csv.writer(output_file, delimiter="\t")
            wr.writerow(Position_of_ShipmentDownload_Header_label)
            for read_row in read_csv:
                write_csv.writerow(read_row)

def serialize_msg_structure():
    with open('ShipmentDownload.ser', 'wb') as f:
        # order of msg_structure is changed as it is sorted in place
        for each_msg_line in msg_structure:
            pickle.dump(each_msg_line.as_dict(), f)

# messageIO
# load from file
def deserialize_msg_structure():
    objects = []
    global msg_structure_reload;
    with open('ShipmentDownload.ser', 'rb') as openfile:
        while True:
            try:
                objects.append(pickle.load(openfile))
            except EOFError:
                break

    for each_obj in objects:
        msg_line_as_json = json.dumps(each_obj)
        create_line_on_deserialization = ShipmentDownload.from_json(msg_line_as_json)
        msg_structure_reload.append(create_line_on_deserialization)

def create_zip_file():
    pass

def number_of_lines(lines_to_generate=1):
    for each_line in range(lines_to_generate):
        gen_line_with_attribute = ShipmentDownload()
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

# read_schema = StringIO(poack)
# spec = json.load(read_schema)
# # Use the above json-spec to build the template class and corresponding object
# builder = pjs.ObjectBuilder(spec)
# ns = builder.build_classes()
# poack = ns.SapPurcahseorderack
# poack_message_structure = []

# def enrich_msg_lines_do(gen_line_with_attribute, zip_line):
#     gen_line_with_attribute["ORDER_ID"] = zip_line[0]
#     gen_line_with_attribute["CANCELLED_FLAG"] = zip_line[1]
#     gen_line_with_attribute["CONFIRMED_AS_ORDERED"] = zip_line[2]
#     gen_line_with_attribute["DELIVERY_CANCELLED_FLAG"] = zip_line[3]
#     gen_line_with_attribute["DELIVERY_DATETIME"] = zip_line[4]
#     gen_line_with_attribute["DELIVERY_DATE_TYPE"] = zip_line[5]
#     gen_line_with_attribute["QUANTITY"] = zip_line[6]
#     poack_message_structure.append(gen_line_with_attribute)
#     return poack_message_structure

# gen_line_with_attribute_poack = poack()

# def read_poack_data():
#     global gen_line_with_attribute_poack
#     loc_file = open("IT-KORTAB", "r")
#     logger.console ("reading file corresponding to po-ack ... ")
#     loc_file_as_string = loc_file.read()
#     logger.console ("reding file ..")
#     d = StringIO(unicode(loc_file_as_string))
#     logger.console ("table as unicode string is {}".format(d))
#     fields = gen_line_with_attribute_poack.keys()
#     df = pd.read_csv(d, usecols=fields, delimiter='\t', dtype=str)
#     for index, each_row in df.iterrows():
#         data_zipped = []
#         data_zipped.append(each_row["ORDER_ID"])
#         data_zipped.append(each_row["CANCELLED_FLAG"])
#         data_zipped.append(each_row["CONFIRMED_AS_ORDERED"])
#         data_zipped.append(each_row["DELIVERY_CANCELLED_FLAG"])
#         data_zipped.append(each_row["DELIVERY_DATETIME"])
#         data_zipped.append(each_row["DELIVERY_DATE_TYPE"])
#         data_zipped.append(each_row["QUANTITY"])
#         gen_line_with_attribute_hub = poack()
#         enrich_msg_lines_do(gen_line_with_attribute_hub, data_zipped)

# count = 0

# def f3(msg_line):
#     global count
#     return (msg_line["Order Number"] == poack_message_structure[count]["ORDER_ID"])

# def f4(msg_line):
#     global count
#     return (msg_line["Promise Qty"] == poack_message_structure[count]["QUANTITY"])

# def simple_filter(filters, msg_structure):
#     for f in filters:
#         msg_structure = filter(f, msg_structure)
#         if not msg_structure:
#             return msg_structure
#     return msg_structure

# def validate_input_output():
#     deserialize_msg_structure()
#     read_poack_data()
#     filtered_rows = []
#     global msg_structure_reload
#     logger.console ("length of msg lines to verify {}".format(len(poack_message_structure)))
#     for i in range(0,len(poack_message_structure)-1):
#         global count
#         count = count + 1
#         logger.info ("validating data {}".format(count))
#         msg_structure_filtered = simple_filter([f3, f4], msg_structure_reload)
#         if len(msg_structure_filtered) > 0:
#             filtered_rows.append (msg_structure_filtered)
#     logger.console ("Length of filtered rows are {}".format(len(filtered_rows)))
#     assert len(filtered_rows) == count

if __name__ == "__main__":
    number_of_lines(4)