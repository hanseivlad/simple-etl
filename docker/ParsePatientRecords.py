#!/usr/bin/env python
# Copyright 2016 Amazon.com, Inc. or its
# affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License is
# located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
import os
import json
# in real life, I would probably use import ujson as json since ujson is much faster, but for the purpose of this exercise
# the default json library is sufficient
import urllib
import boto3
import sys
import os
import csv

output_dir = '/processed'
input_bucket_name = os.environ['s3InputBucket']
output_bucket_name = os.environ['s3OutputBucket']
sqsqueue_name = os.environ['SQSBatchQueue']
aws_region = os.environ['AWSRegion']
s3 = boto3.client('s3', region_name=aws_region)
sqs = boto3.resource('sqs', region_name=aws_region)

def create_dirs():
    for dirs in [output_dir]:
        if not os.path.exists(dirs):
            os.makedirs(dirs)

def parse_patient_data(input_file, output_file):
    """Process the HL7 bundle as json and normalize it into csv

    There are multiple ways to convert json to csv. 2 of them:
    1. No your data, extract just the data you need, and export it.
    2. Perform a 2-pass scan. 1st pass figure out the superset of every possible field in all records together.
       Then on 2 pass, extract all fields from all records and write them to a csv
    There are good use cases for both. If the goal is to save the raw data, it should be saved to a data lake or inserted in some  sort
    of a document database, like ES or Cassandra. I believe that we are looking for very specific normalized data. Because of this and
    performance reasons, I believe that option (1) makes a lot more sense.

    Also, I am making a lot of assumptions of what makes sense to normalize here:
    1. Input file must be properly formatted: there should be an entry section, a list of entries, and a resource block.
    2. We do not want to deal with robots or animals, hence only entries with resourceType of patient are accepted.
    3. Each file needs to include at least one patient.
    In a real error scenario, it would probably not make sense to re-parse the same bad file over and over again when input file fails
    either (1) or (2) or (3).  I would rather send an email or another kind of notification to devops to have them look at the problem
    and produce a good file. In mind view, that would be outside of the scope for this exercise.

    :param input_file: the json file to parse
    :param output_file: the csv file to export
    :return: returns nothing
    """
    rows = []
    if os.path.exists(input_file):
        with open(input_file) as f:
            data = json.loads(f.read())
            for entry in data.get('entry', []):
                url = entry.get('fullUrl', '')
                resource = entry.get('resource', {})
                if not resource:
                    print('resource not found for url %s in %s: please check' % (url, in_file), file=sys.stderr)
                    continue

                resource_type = resource.get('resourceType', '').lower()
                if resource_type != 'patient':
                    print('resource type is not patient for url %s in %s: please check' % (url, in_file), file=sys.stderr)
                    continue

                resource_id = resource.get('id', '')
                last_updated = resource.get('meta', {}).get('lastUpdated', '')
                status = resource.get('text', {}).get('status', '').lower()
                system_id = resource.get('text', {}).get('value')
                active = resource.get('active', False)
                first_name = resource.get('name', {})[0].get('given', [''])[0]
                last_name = resource.get('name', {})[0].get('family', [''])
                phone = resource.get('telecom', [{}])[0].get('value', '')
                gender = resource.get('gender', '')
                address = resource.get('address', [{}])[0].get('value', '')

                rows.append([url, resource_id, last_updated, status, system_id, active, first_name, last_name, phone, gender, address])

    with open(output_file, 'w') as f:
        writer = csv.writer(f, dialect=csv.excel)
        writer.writerow(['url', 'resource_id', 'last_updated', 'status', 'system_id', 'active', 'first_name', 'last_name', 'phone', 'gender', 'address'])
        for row in rows:
            writer.writerow(row)

def process_data():
    """Process patient data files

    In case of error we'll put the message back in the queue and make it visable again. It will end up in
    the dead letter queue after five failed attempts.

    In a real error scenario, there is a small chance of:
    1. failed s3 connectivity or
    2. the object being deleted prior to us processing
    In case of intermittent network outages, it probably makes sense to implement an exponential back-off stragegy.
    This wouldn't help if the file was deleted, in which case it's best to send the message straight to the dead letter queue and 
    notify persons involved to investigate.
    """
    for message in get_messages_from_sqs():
        try:
            message_content = json.loads(message.body)
            input_file = urllib.unquote_plus(message_content
                                        ['Records'][0]['s3']['object']
                                        ['key']).encode('utf-8')
            s3.download_file(input_bucket_name, input_file, input_file)
            output_file = os.path.join(output_dir, os.path.splitext(input_file)[0]+'.csv')
            parse_patient_data(input_file, output_file)
            upload_data(output_file)
            cleanup_files(input_file, output_file)
        except:
            message.change_visibility(VisibilityTimeout=0)
            continue
        else:
            message.delete()

def cleanup_files(input_file, output_file):
    os.remove(input_file)
    os.remove(output_dir + '/' + output_file)

def upload_data(output_file):
    s3.upload_file(output_file, output_bucket_name, os.path.basename(output_file))

def get_messages_from_sqs():
    results = []
    queue = sqs.get_queue_by_name(QueueName=sqsqueue_name)
    for message in queue.receive_messages(VisibilityTimeout=120,
                                          WaitTimeSeconds=20,
                                          MaxNumberOfMessages=10):
        results.append(message)
    return(results)

def main():
    create_dirs()
    while True:
        process_data()

if __name__ == "__main__":
    main()
