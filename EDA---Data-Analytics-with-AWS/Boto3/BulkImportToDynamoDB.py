#
#   Requirements: Amazon CLI must be installed and Boto 3 must be installed
#   Before adding data, check if evt_uuid (from the CSV) is added as "Primary partition key" in DynamoDB
#   Based on Tutorial: https://www.youtube.com/watch?v=DqLFfp3Yg_g
#

import boto3
import csv

# Location must be set corretly to work
dynamodb = boto3.resource('dynamodb','eu-west-1')

# Write data into DynamoDB
def batch_write(table_name,rows):
    table = dynamodb.Table(table_name)

    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)
    return True

# Reading CSV line by line and adding to list
def read_csv(csv_file,list):
    rows = csv.DictReader(open(csv_file))

    for row in rows:
        list.append(row)

if __name__ == '__main__':

    # Table name of DynamoDB
    table_name = 'unicamsensordata'
    file_name = '../rawdata/2020_03_01T00_00_00_2020_04_01T00_00_00.csv'
    items = []

    read_csv(file_name, items)
    status = batch_write(table_name, items)

    if (status):
        print('Data is saved')
    else:
        print('Error while inserting data')