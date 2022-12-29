from __future__ import print_function
import sys
import struct
import csv
import boto3
import urllib.parse
import time, urllib
import pandas as pd
import awswrangler as wr
import datetime
import time
import os
import json


print("Loading Function..")

# landing bucket and prefix
LANDING_BUCKET = os.environ.get('LANDING_BUCKET')
RAW_BUCKET = os.environ.get('RAW_BUCKET')
TZ = os.environ.get('TZ')

# S3 Object
s3 = boto3.client('s3')
s3_object = boto3.client('s3', region_name='us-west-2')
s3_resource = boto3.resource('s3')


# convert timestamp to string
def timestamp2string(timeStamp):
    try:
        d = datetime.datetime.fromtimestamp(timeStamp)
        str1 = d.strftime("%d/%m/%Y %H:%M:%S")
        return str1
    except Exception as e:
        print(e)
        return ''


def lambda_handler(event, context):
    print("=====================================================")
    print(event)
    # get log file from raw bucket
    body = event['Records'][0]['body']
    b = json.loads(body)
    object_key = urllib.parse.unquote_plus(b['Records'][0]['s3']['object']['key'], encoding='utf-8')

    event_bucket = b['Records'][0]['s3']['bucket']['name']
    s3_opt = b['Records'][0]['eventName']
    if 'ObjectCreated' in s3_opt and RAW_BUCKET == event_bucket:
        # access parquet file
        # s3_path = "s3://" + RAW_BUCKET + '/' + object_key
        # df_parquet = wr.s3.read_parquet(path=s3_path)
        content_object = s3_resource.Object(RAW_BUCKET, object_key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)

        # get existed files in landing bucket
        str1 = ''
        land_dir = str1.join(object_key.split('/')[0:-1])
        land_bucket = s3_resource.Bucket(LANDING_BUCKET)
        exsit_file_list = []
        for object_summary in land_bucket.objects.filter(Prefix=land_dir + '/Stationary/'):
            exsit_file_list.append(object_summary.key)
        print(exsit_file_list)

        # filter VehSpeed
        veh_speed = json_content['speed']
        speed_list = [e['value'] for e in veh_speed]

        if not veh_speed.empty:
            # Set up filename format
            start_time_str = timestamp2string(veh_speed[0]['timestamp'])
            start_date = (start_time_str.split(' ')[0]).split('-')[2]
            start_month = (start_time_str.split(' ')[0]).split('/')[1]
            start_year = (start_time_str.split(' ')[0]).split('/')[0]
            filename = 'canserver-events_' + start_year + '-' + start_month + '-' + start_date + '.json'

            start_index = speed_list.index(0)
            end_index = len(speed_list) - 1
            start_time = veh_speed[start_index]['timestamp']
            end_time = 0.0
            time_list = []
            for i in range(start_index + 1, end_index):
                cur_time = veh_speed[i]['timestamp']
                cur_speed = speed_list[i]
                pre_speed = speed_list[i - 1]
                next_speed = speed_list[i + 1]

                if cur_speed == 0 and pre_speed != 0:
                    start_time = cur_time
                elif cur_speed == 0 and (cur_time - start_time) <= 12:
                    continue
                elif cur_speed == 0 and (cur_time - start_time) >= 13:
                    if next_speed != 0:
                        end_time = cur_time
                        time_list.append({"start": (start_time + 3), "end": (end_time - 3)})
                    elif next_speed == 0 and i == end_index - 1:
                        end_time = cur_time
                        time_list.append({"start": (start_time + 3), "end": (end_time - 3)})
                    else:
                        continue
                else:
                    continue

            # Json format
            data_dict = {
                "IMU-telematics": {
                    "stationary-state": []
                }
            }
            data_dict["IMU-telematics"]["stationary-state"] = time_list
            print(time_list)
            if time_list:
                # combine old files
                path = land_dir + '/Stationary/' + filename
                if path in exsit_file_list:
                    print("file need to be updated")
                    last_file = wr.s3.read_json(path='s3://' + LANDING_BUCKET + '/' + path)
                    last_file = last_file.to_dict()
                    if last_file["IMU-telematics"]["stationary-state"][-1]["end"] <= \
                            data_dict["IMU-telematics"]["stationary-state"][0]["start"]:
                        last_file["IMU-telematics"]["stationary-state"] += data_dict["IMU-telematics"]["stationary-state"]
                        data_dict = last_file
                        print("File updated")
                    elif last_file["IMU-telematics"]["stationary-state"][0]["start"] >= \
                            data_dict["IMU-telematics"]["stationary-state"][-1]["end"]:
                        data_dict["IMU-telematics"]["stationary-state"] += last_file["IMU-telematics"]["stationary-state"]
                        print("File updated")
                    else:
                        pass

                data_string = json.dumps(data_dict, indent=2)
                try:
                    s3.put_object(
                        Body=data_string,
                        Bucket=LANDING_BUCKET,
                        Key=path
                    )
                except Exception as e:
                    print(e)
                    print("writing to json failed!!!")