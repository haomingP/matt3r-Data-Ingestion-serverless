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

    # get log file from raw bucket
    body = event['Records'][0]['body']
    b = json.loads(body)
    Message = b['Message']
    mess = json.loads(Message)
    object_key = urllib.parse.unquote_plus(mess['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # access parquet file
    s3_path = "s3://" + RAW_BUCKET + '/' + object_key
    df_parquet = wr.s3.read_parquet(path=s3_path)

    str = ''
    land_dir = str.join(object_key.split('/')[0:-1])
    land_bucket = s3_resource.Bucket(LANDING_BUCKET)
    exsit_file_list = []
    for object_summary in land_bucket.objects.filter(Prefix=land_dir):
        exsit_file_list.append(object_summary.key)

    # filter autopilot
    df_filtered = df_parquet[df_parquet['field'] == 'AutopilotState']

    if not df_filtered.empty:
        autopilot_df = pd.DataFrame()
        for i in range(1, df_filtered.shape[0]):
            if df_filtered.iloc[i, 3] == 3.0 and df_filtered.iloc[i - 1, 3] == 2.0:
                new_df = pd.DataFrame(df_filtered.iloc[i]).T
                new_df['Status'] = 'engagement'
                autopilot_df = pd.concat([autopilot_df, new_df])
            elif df_filtered.iloc[i, 3] <= 2.0 and df_filtered.iloc[i - 1, 3] == 3.0:
                new_df = pd.DataFrame(df_filtered.iloc[i]).T
                new_df['Status'] = 'disengagement'
                autopilot_df = pd.concat([autopilot_df, new_df])
            else:
                pass

        # Convert to Json
        autopilot_dict = {"auditory": {}}
        for index, row in autopilot_df.iterrows():
            tmp_dict = {}
            tmp_dict[row['Status']] = [{"timestamp": row['timestamp'], "canbus_state": row['value']}]
            autopilot_dict["auditory"].update(tmp_dict)
        print(autopilot_dict)

        # check if have engage / disengage in this day
        if not autopilot_df.empty:
            # check if need to combine
            start_time_str = timestamp2string(df_filtered.iloc[0, 1])
            start_date = (start_time_str.split(' ')[0]).split('/')[0]
            start_month = (start_time_str.split(' ')[0]).split('/')[1]
            start_year = (start_time_str.split(' ')[0]).split('/')[2]
            filename = 'canserver-events_' + start_year + '-' + start_month + '-' + start_date + '.json'
            path = land_dir + '/' + filename
            if path in exsit_file_list:
                last_file = wr.s3.read_json(path='s3://' + LANDING_BUCKET + '/' + path)
                for k1 in last_file["auditory"].keys():
                    for k2 in autopilot_dict["auditory"].keys():
                        if k1 == k2:
                            last_file["auditory"][k1].append(autopilot_dict["auditory"][k2])
                autopilot_dict = last_file

            data_string = json.dumps(autopilot_dict, indent=2, default=str)
            try:
                s3.put_object(
                    Body=data_string,
                    Bucket=LANDING_BUCKET,
                    Key=land_dir + '/' + filename
                )
            except Exception as e:
                print(e)
                print("writing to json failed!!!")