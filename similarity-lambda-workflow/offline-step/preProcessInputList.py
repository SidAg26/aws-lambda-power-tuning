import boto3

s3 = boto3.client('s3')

# Function to list all .jpg objects in the bucket
def list_jpg_objects(bucket, prefix):
    
    jpg_objects = []
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.jpg'):
                    jpg_objects.append(obj['Key'])
    return jpg_objects


def lambda_handler(event, context):
    input_dict = event['payload'] if 'payload' in event else None
    input_dict = event['payloads3'] if 'payloads3' in event else input_dict
    input_list = None
    if 'payloads3' in event and isinstance(input_dict, dict):
        input_list = list_jpg_objects(event['payloads3']['bucket'], event['payloads3']['prefix'])
        print(f'Found {len(input_list)} jpg objects in bucket {event['payloads3']["bucket"]}')
    elif 'payload' in event:
        # Do something with jpg_objects
        if isinstance(input_dict, dict):
            i_min = input_dict['min'] if 'min' in input_dict else 0
            i_max = input_dict['max'] if 'max' in input_dict else 100
            i_step = input_dict['stepSize'] if 'stepSize' in input_dict else None
        else:
            i_min = 1
            i_max = 100
            i_step = None
        if i_step:
            input_list = [i for i in range(i_min, i_max, i_step)]
        else:
            input_list = [i_min, i_max]
    else:
        input_list = []

    return input_list