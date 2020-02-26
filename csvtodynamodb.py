from __future__ import print_function
from __future__ import division #Python 2/3 compatiblity for integer division
import boto3
import sys
import argparse
import csv
import time
from decimal import *

'''
This module loads the content of a CSV file into a dynamodb table

'''


def main(arguments):

    #cmdline arguments
    parser = argparse.ArgumentParser(
        description="Loads the content of a CSV file into a dynamodb table",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--table-name', required=True, help="Name of the DynamoDB table")
    parser.add_argument('--data-file', required=True, help="Path to a CSV file containing data to be loaded")
    parser.add_argument('--write-rate', required=False, default=5, type=int, help="Number of records to write in table per second (default:5)")
    parser.add_argument('--delimiter', default=',', nargs='?', help='Delimiter for csv records (default=,)')
    parser.add_argument('--profile', required=True,help="AWS CLI profile to be used")
    parser.add_argument('--region', required=True, help="AWS region where table is located")
    args = parser.parse_args(arguments)

    print(args)

    # dynamodb and table initialization
    session = boto3.Session(profile_name=args.profile,region_name=args.region)
    endpointUrl = "https://dynamodb." + args.region + ".amazonaws.com"
    dynamodb = session.resource('dynamodb', endpoint_url=endpointUrl)
    table = dynamodb.Table(args.table_name)

    # reads csv
    with open(args.data_file) as csvfile:
#        tokens = csv.reader(csvfile, delimiter=args.delimiter, encoding='utf-8-sig')
        tokens = csv.reader(csvfile, delimiter=args.delimiter)
        # read first line in file which contains dynamo db field names
        header = next(tokens)
        # read second line in file which contains dynamo db field data types
        headerFormat = next(tokens)
        # rest of file contain new records
        for token in tokens:
            item = {}
            for i,val in enumerate(token):
                if val:
                    key = header[i]
                    if headerFormat[i]=='numeric':
                        try:
                            val = int(val)
                        except ValueError:
                            val = Decimal(val)
                    elif headerFormat[i]=='boolean':
                        val = True if val.lower() == 'true' else False
                    elif headerFormat[i]=='list':
                        mylist = []
                        records = val.split(';')
                        for e in records:
                            subitem={}
                            rows = e.split(',')
                            for r in rows:
                                att = r.split("?")
                                subitem[att[0]] = att[1]
                            mylist.append(subitem)
                        val = mylist
                    item[key] = val
            print(item)
            table.put_item(Item = item)
            time.sleep(1/args.write_rate) # to accomodate max write provisioned capacity for table

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
