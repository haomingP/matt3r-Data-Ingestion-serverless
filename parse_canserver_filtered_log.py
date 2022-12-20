from __future__ import print_function
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
RAW_BUCKET = os.environ.get('RAW_BUCKET')
LANDING_BUCKET = os.environ.get('LANDING_BUCKET')
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


# get the start and end time of this log file
def get_start_end_time(df):
    start_time_str = timestamp2string(df.iloc[0, 1])
    start_time = start_time_str.split(' ')[1]
    start_hour = start_time.split(':')[0]
    start_date = (start_time_str.split(' ')[0]).split('/')[0]

    end_time_str = timestamp2string(df.iloc[-1, 1])
    end_time = end_time_str.split(' ')[1]
    end_hour = end_time.split(':')[0]

    file_str = 'canserver_'
    time_list = df['timestamp'].to_list()
    new_time_list = [int((timestamp2string(e).split(' ')[1]).split(':')[0]) +
                     (24 * (int((timestamp2string(e).split(' ')[0]).split('/')[0]) - int(start_date))) for e in
                     time_list]
    target_list = sorted(list(set(new_time_list)))
    s_e_list = []
    for t in target_list:
        s_e_list.append(helper_search_hour(new_time_list, t))

    filename_list = []
    for l in s_e_list:
        time_str = timestamp2string(df.iloc[l[0], 1])
        hour = int((time_str.split(' ')[1]).split(':')[0])
        date = (time_str.split(' ')[0]).split('/')[0]
        month = (time_str.split(' ')[0]).split('/')[1]
        year = (time_str.split(' ')[0]).split('/')[2]
        if hour >= 9:
            filename_list.append(file_str + year + '-' + month + '-' + date + '_' + str(hour + 1))
        else:
            filename_list.append(file_str + year + '-' + month + '-' + date + '_' + '0' + str(hour + 1))
    print(filename_list)

    return s_e_list, filename_list


# Helper binarysearch function
def helper_search_hour(nums, target):
    def search_index(nums, target, sign):
        left = 0
        right = len(nums)
        while left < right:
            mid = (left + right) // 2
            if nums[mid] > target or (sign and target == nums[mid]):
                right = mid
            else:
                left = mid + 1
        return left

    left_index = search_index(nums, target, True)

    if left_index == len(nums) or nums[left_index] != target:
        return [-1, -1]

    right_index = search_index(nums, target, False) - 1
    return [left_index, right_index]


def lambda_handler(event, context):
    print("=====================================================")
    print(event)

    body = event['Records'][0]['body']
    b = json.loads(body)
    object_key = urllib.parse.unquote_plus(b['Records'][0]['s3']['object']['key'], encoding='utf-8')

    source_bucket = RAW_BUCKET
    event_bucket = b['Records'][0]['s3']['bucket']['name']
    print(event_bucket)
    s3_opt = b['Records'][0]['eventName']

    # Only uploead will trigger function
    if 'ObjectCreated' in s3_opt and source_bucket == event_bucket:
        # access file
        get_file = s3_object.get_object(Bucket=source_bucket,
                                        Key=object_key)
        print("Successfully got the log files!!!")

        # get file content
        get = get_file['Body']
        names = ['timestamp', 'field', 'value']
        df_parquet = pd.read_csv(get, delimiter=",", header=None)
        df_parquet = df_parquet.rename(columns={0: names[0], 1: names[1], 2: names[2]})
        df_parquet = df_parquet.reset_index()
        df_parquet = pd.DataFrame(df_parquet)
        if len(df_parquet.iloc[0, 1]) >= 15:
            print(df_parquet.iloc[0, 1])
            df_parquet.iloc[0, 1] = df_parquet.iloc[0, 1][-14:]
            print(df_parquet.iloc[0, 1])
        df_parquet[['timestamp']] = df_parquet[['timestamp']].astype(float)

        # Merge files which in same hour
        split_list, fn_list = get_start_end_time(df_parquet)

        # Get the landing bucket path
        str = ''
        land_dir = str.join(object_key.split('/')[0:-1])
        landing_bucket = s3_resource.Bucket(LANDING_BUCKET)
        exsit_file_list = ['']
        for object_summary in landing_bucket.objects.filter(Prefix=land_dir):
            exsit_file_list.append(object_summary.key)

        for i in range(len(split_list)):
            part_df = df_parquet.iloc[split_list[i][0]:split_list[i][1] + 1, :]

            # search if have previous data of this hour
            name = land_dir + '/' + fn_list[i] + '-00-00' + '.parquet'
            if name in exsit_file_list:
                print(name, " is exist in Landing Bucket, need to be updated")
                last_file = wr.s3.read_parquet(path='s3://' + LANDING_BUCKET + '/' + name)
                if last_file.iloc[0, 1] <= part_df.iloc[0, 1] and last_file.iloc[-1, 1] >= part_df.iloc[-1, 1]:
                    continue
                elif last_file.iloc[-1, 1] < part_df.iloc[0, 1]:
                    part_df = pd.concat([last_file, part_df])
                elif last_file.iloc[0, 1] < part_df.iloc[-1, 1]:
                    part_df = pd.concat([part_df, last_file])
                print("Updated successfully!")
            else:
                print("new file!")
            landing_path = 's3://' + LANDING_BUCKET + '/' + land_dir + '/' + fn_list[i] + '-00-00' + '.parquet'

            try:
                wr.s3.to_parquet(
                    df=part_df,
                    path=landing_path,
                    dataset=False
                )
                print("Parquet file has been saved into landing bucket!!!")
            except Exception as e:
                print(e)
                print("writing to parquet failed!!!")