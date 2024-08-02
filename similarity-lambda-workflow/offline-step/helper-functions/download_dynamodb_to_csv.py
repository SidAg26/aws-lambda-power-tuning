import boto3
import csv
from boto3.dynamodb.conditions import Attr

def download_all_dynamodb_records(table_name):
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
    table = dynamodb.Table(table_name)
    
    all_records = []
    column_name = 'start_time'
    last_evaluated_key = None

    while True:
        if last_evaluated_key:
            response = table.scan(
                FilterExpression=Attr(column_name).exists(),
                ExclusiveStartKey=last_evaluated_key
            )
        else:
            response = table.scan(
                FilterExpression=Attr(column_name).exists()
            )

        all_records.extend(response['Items'])

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

    return all_records

# Example usage
table_name = 'linpack_logs'
all_records = download_all_dynamodb_records(table_name)

# Specify the file path and name
csv_file = 'linpack_records_July30.csv'

# Get all unique keys from all records
all_keys = set()
for record in all_records:
    all_keys.update(record.keys())

# Write the records to the CSV file
with open(csv_file, 'w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=all_keys)
    writer.writeheader()
    for record in all_records:
        # Fill missing values with an empty string
        for key in all_keys:
            if key not in record:
                record[key] = ''
        writer.writerow(record)

print(f"Records have been written to {csv_file}")