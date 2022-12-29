import json
import sys
import struct
import os
import datetime
import csv
import boto3
import awswrangler as wr
import pandas as pd
import time

# from bitstring import Bits

RAW_BUCKET = "haoming-raw-test"
LANDING_BUCKET = "haoming-test-bucket"


# get the start and end time of this log file
def get_start_end_time(df):
    start_time_str = df.iloc[0, 0]
    start_time = start_time_str.split(' ')[1]
    start_hour = start_time.split(':')[0]
    start_date = (start_time_str.split(' ')[0]).split('-')[2]

    end_time_str = df.iloc[-1, 0]
    end_time = end_time_str.split(' ')[1]
    end_hour = end_time.split(':')[0]

    file_str = 'canserver_'
    time_list = df['timestamp'].to_list()
    new_time_list = [int((e.split(' ')[1]).split(':')[0]) +
                     (24 * (int((e.split(' ')[0]).split('-')[2]) - int(start_date))) for e in
                     time_list]
    target_list = sorted(list(set(new_time_list)))
    s_e_list = []
    for t in target_list:
        s_e_list.append(helper_search_hour(new_time_list, t))

    filename_list = []
    for l in s_e_list:
        time_str = df.iloc[l[0], 0]
        hour = int((time_str.split(' ')[1]).split(':')[0])
        date = (time_str.split(' ')[0]).split('-')[2]
        month = (time_str.split(' ')[0]).split('-')[1]
        year = (time_str.split(' ')[0]).split('-')[0]
        if hour >= 9:
            filename_list.append(file_str + year + '-' + month + '-' + date + '_' + str(hour + 1))
        else:
            filename_list.append(file_str + year + '-' + month + '-' + date + '_' + '0' + str(hour + 1))

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


def string2timestamp(ts):
    """
    convert string to timestamp

    param ts: time string
    """
    timestring = ts.split('.')
    timeArray = time.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
    timeStamp = int(time.mktime(timeArray))
    timeStamp += int(timestring[-1]) / 1000
    return timeStamp


def bin_to_dec(str1):
    sum = 0
    lenth = len(str1)
    for i in range(1, lenth):
        if str1[i] == '1':
            save = 2 ** (lenth - i - 1)
            sum = sum + save

    if str1[0] == '1':
        return sum - 2 ** (lenth - 1)
    else:
        return +sum


# Main lambda function
def lambda_handler(event, context):
    DATA_DICT = {921: 'autopilot', 273: 'accelerometer', 257: 'angular_velocity', 79: 'gps', 599: 'speed'}
    AP_STATE_DICT = {0: 'DISABLED', 1: 'UNAVAILABLE', 2: 'AVAILABLE', 3: 'ACTIVE_NOMINAL', 4: 'ACTIVE_RESTRICTED',
                     5: 'ACTIVE_NAV', 8: 'ABORTING', 9: 'ABORTED'}
    ACC_SCALE = 0.00125
    YAW_SCALE = 0.0001
    PITCH_ROLL_SCALE = 0.00025
    SPEED_SCALE = 0.08
    SPEED_OFFSET = -40.0
    GNNS_FACTOR = 1e-6
    MAX_SR = 1.2

    print("<================================================>")
    print(event)
    bucket = RAW_BUCKET
    body = event['Records'][0]['body']
    b = json.loads(body)
    key = urllib.parse.unquote_plus(b['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # key = event['Records'][0]['s3']['object']['key']

    # Use the boto3 client for S3 to download the file
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    response = s3.get_object(Bucket=bucket, Key=key)

    # Get the file's content as a bytearray
    file = response['Body']
    print(type(file))

    csv_filedsnames = ['timestamp', 'long_acc', 'lat_acc', 'vert_acc', 'acc_unit', 'yaw_rate', 'pitch_rate',
                       'roll_rate', 'gyro_unit', 'lat', 'long', 'speed', 'speed_unit', 'AP_status']
    lastSyncTime = 0

    outputfile = None
    outputFilename = os.path.splitext(key)[0] + '.csv'
    print("outputFilename: ", outputFilename)

    def parse_and_insert(frameid, payload, epoch_time, epoch_dict, convert_time=False):
        if convert_time:
            epoch_dict[epoch_time][0] = datetime.datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S.%f')[
                                        :-3]
        if frameid == 273:
            long_acc = ACC_SCALE * int.from_bytes(payload[0:2], 'little', signed=True)
            lat_acc = ACC_SCALE * int.from_bytes(payload[2:4], 'little', signed=True)
            vert_acc = ACC_SCALE * int.from_bytes(payload[4:6], 'little', signed=True)
            epoch_dict[epoch_time][1:5] = long_acc, lat_acc, vert_acc, 'm/s^2'

        elif frameid == 257:
            # print(bin(payload[3]))
            yaw_rate = YAW_SCALE * int.from_bytes(payload[0:2], 'little', signed=True)
            pitch_str = '{0:08b}'.format(payload[3])[1:] + '{0:08b}'.format(payload[2])
            roll_str = '{0:08b}'.format(payload[5])[2:] + '{0:08b}'.format(payload[4]) + '{0:08b}'.format(payload[3])[0]
            pitch_int = bin_to_dec(pitch_str)
            roll_int = bin_to_dec(roll_str)
            pitch_rate = PITCH_ROLL_SCALE * pitch_int
            roll_rate = PITCH_ROLL_SCALE * roll_int
            epoch_dict[epoch_time][5:9] = yaw_rate, pitch_rate, roll_rate, 'rad/s'

        elif frameid == 599:
            speed = SPEED_SCALE * int('{0:08b}'.format(payload[2]) + '{0:08b}'.format(payload[1])[:4], 2) + SPEED_OFFSET
            epoch_dict[epoch_time][11:13] = speed, 'KPH'

        elif frameid == 79:
            lat_str = '{0:08b}'.format(payload[3])[4:] + '{0:08b}'.format(payload[2]) + '{0:08b}'.format(
                payload[1]) + '{0:08b}'.format(payload[0])
            long_str = '{0:08b}'.format(payload[6]) + '{0:08b}'.format(payload[5]) + '{0:08b}'.format(
                payload[4]) + '{0:08b}'.format(payload[3])[:4]
            lat_int = bin_to_dec(lat_str)
            long_int = bin_to_dec(long_str)
            lat = GNNS_FACTOR * lat_int
            long = GNNS_FACTOR * long_int
            epoch_dict[epoch_time][9:11] = lat, long

        elif frameid == 921:
            ap_state = int('{0:08b}'.format(payload[0])[4:], 2)
            epoch_dict[epoch_time][13] = AP_STATE_DICT[ap_state]

    headerData = file.read(22)
    print("Read successfully!")
    print(headerData)
    if (len(headerData) == 22):
        # Check to see if our header matches what we expect
        if (headerData == b'CANSERVER_v2_CANSERVER'):
            binary_df = pd.DataFrame(columns=csv_filedsnames)
            outputfile = binary_df
            print("File created!")
            pass
        else:
            print("Not a valid CANServer v2 file.  Unable to convert", file=sys.stderr)
            exit(1)
    current_row = ['NA'] * len(csv_filedsnames)
    epoch_dict = {}
    min_epoch = 0
    while True:
        # Look for the start byte
        byteRead = file.read(1)
        if len(byteRead) == 1:
            if (byteRead == b'C'):
                # check to see if we have a header.
                goodheader = False

                # read 21 more bytes
                possibleHeader = file.read(21)
                if (len(possibleHeader) == 21):
                    if (possibleHeader == b'ANSERVER_v2_CANSERVER'):
                        # we found a header (this might have been because of just joining multiple files togeather)
                        goodheader = True
                        pass

                if (goodheader):
                    # header was valid.  Just skip on ahead
                    pass
                else:
                    # we didn't see the header we expected.  Seek backwards the number of bytes we read
                    file.seek(-len(possibleHeader), 1)
            elif (byteRead == b'\xcd'):
                # this is a mark message.  The ASC format doesn't seem to have any comments or anything so we can't directly convert this mark
                # Instead we create a new output file with the markstring as part of its filename
                marksize = file.read(1)
                marksize = int.from_bytes(marksize, 'big')
                markdata = file.read(marksize)

                markString = markdata.decode("ascii")
                print("Parsing the log with markString: ", markString)

            elif (byteRead == b'\xce'):
                # this is running time sync message.
                timesyncdata = file.read(8)

                if len(timesyncdata) == 8:
                    lastSyncTime = struct.unpack('<Q', timesyncdata)[0]
                else:
                    print("Time Sync frame read didn't return the proper number of bytes", file=sys.stderr)

            elif (byteRead == b'\xcf'):
                # we found our start byte.  Read another 5 bytes now
                framedata = file.read(5)
                if len(framedata) == 5:
                    unpackedFrame = struct.unpack('<2cHB', framedata)
                    # print(unpackedFrame)

                    frametimeoffset = int.from_bytes(unpackedFrame[0] + unpackedFrame[1], 'little')
                    # convert the frametimeoffset  from ms to us
                    frametimeoffset = frametimeoffset * 1000

                    frameid = unpackedFrame[2]

                    framelength = unpackedFrame[3] & 0x0f
                    busid = (unpackedFrame[3] & 0xf0) >> 4
                    if (framelength < 0):
                        framelength = 0
                    elif (framelength > 8):
                        framelength = 8

                    framepayload = file.read(framelength)
                    if frameid in DATA_DICT:
                        frametime = lastSyncTime + frametimeoffset
                        epoch_time = frametime / 1000000
                        date_time = datetime.datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        if epoch_time in epoch_dict:
                            parse_and_insert(frameid=frameid, payload=framepayload, epoch_time=epoch_time,
                                             epoch_dict=epoch_dict, convert_time=False)

                        elif min_epoch and epoch_time > min_epoch + MAX_SR:
                            new_df = pd.DataFrame(epoch_dict[min_epoch])
                            new_df = new_df.T
                            new_df.columns = csv_filedsnames
                            outputfile = pd.concat([outputfile, new_df])
                            # csv_writer.writerow(epoch_dict[min_epoch])
                            del (epoch_dict[min_epoch])
                            epoch_dict[epoch_time] = ['NA'] * len(csv_filedsnames)
                            min_epoch = min(epoch_dict, key=epoch_dict.get)
                            parse_and_insert(frameid=frameid, payload=framepayload, epoch_time=epoch_time,
                                             epoch_dict=epoch_dict, convert_time=True)

                        else:
                            epoch_dict[epoch_time] = ['NA'] * len(csv_filedsnames)
                            parse_and_insert(frameid=frameid, payload=framepayload, epoch_time=epoch_time,
                                             epoch_dict=epoch_dict, convert_time=True)
                            if min_epoch == 0:
                                min_epoch = min(epoch_dict, key=epoch_dict.get)
                else:
                    break
        else:
            break
    print(outputfile.shape)
    print('Successfully!!')

    split_list, fn_list = get_start_end_time(outputfile)
    print(split_list)
    print(fn_list)

    # Get the landing bucket path
    str2 = ''
    land_dir = str2.join(key.split('/')[0:-1])
    landing_bucket = s3_resource.Bucket(LANDING_BUCKET)
    exsit_file_list = []
    for object_summary in landing_bucket.objects.filter(Prefix=land_dir):
        exsit_file_list.append(object_summary.key)
    print("exit: ", exsit_file_list)

    for i in range(len(split_list)):
        part_df = outputfile.iloc[split_list[i][0]:split_list[i][1] + 1, :]
        clean_dict = {"accel": [], "gyro": [], "speed": [], "location": [], "ap_status": []}
        for index, row in part_df.iterrows():
            timeStp = string2timestamp(row[0])
            if row[1] != "NA":
                clean_dict["accel"] += [{"timestamp": timeStp, "value": [row[1], row[2], row[3]]}]
            if row[5] != "NA":
                clean_dict["gyro"] += [{"timestamp": timeStp, "value": [row[5], row[6], row[7]]}]
            if row[9] != "NA":
                clean_dict["location"] += [{"timestamp": timeStp, "value": [row[9], row[10]]}]
            if row[11] != "NA":
                clean_dict["speed"] += [{"timestamp": timeStp, "value": row[11]}]
            if row[13] != "NA":
                clean_dict["ap_status"] += [{"timestamp": timeStp, "value": row[13]}]

        # search if have previous data of this hour
        name = land_dir + '/' + fn_list[i] + '-00-00' + '.parquet'
        if name in exsit_file_list:
            print(name, " is exist in Landing Bucket, need to be updated")
            content_object = s3_resource.Object(LANDING_BUCKET, name)
            file_content = content_object.get()['Body'].read().decode('utf-8')
            last_json = json.loads(file_content)
            print(last_json)
            if last_json["location"][0]["timestamp"] >= clean_dict["location"][-1]["timestamp"]:
                for k in clean_dict.keys():
                    clean_dict[k] += last_json[k]
            elif last_json["location"][-1]["timestamp"] <= clean_dict["location"][0]["timestamp"]:
                for k in clean_dict.keys():
                    last_json[k] += clean_dict[k]
                    clean_dict = last_json
            else:
                continue
            print("Updated successfully!")
        else:
            print("new file!")

        landing_path = land_dir + '/' + fn_list[i] + '-00-00' + '.json'
        print(landing_path)
        data_string = json.dumps(clean_dict, indent=2)
        try:
            s3.put_object(
                Body=data_string,
                Bucket=LANDING_BUCKET,
                Key=landing_path
            )
            print("Json file has been saved into landing bucket!!!")
        except Exception as e:
            print(e)
            print("writing to Json failed!!!")
