import json
import sys
import traceback
import boto3
import datetime
import os
import Utils
import CloudwatchWrapper
import KdsWrapper


# initialize client
KdsClient = boto3.client('kinesis');
CwClient = boto3.client('cloudwatch');
KdsArn = os.environ['AWS_KINESIS_STREAM_ARN']
KdsName = Utils.parse_arn(KdsArn)['resource'];


#CwAlarmName = os.environ['AutoScaleKDS-Dev']
CwAlarmName = 'AutoScaleKDS-Dev'

def handler_function(event, context):
    KdsInfo = KdsWrapper.getKdsInfo(KdsName);
    print('KdsInfo ' + json.dumps(KdsInfo, default=datetime_handler));

    KdsUtil = KdsWrapper.getKdsUtilization(KdsName)
    print('KdsUtil ' + json.dumps(KdsUtil, default=datetime_handler));

    CloudwatchWrapper.publishMetrics('Scaling Metrics', KdsName, KdsUtil['ShardCount'], KdsUtil['Utilization'])

    return

def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

json.JSONEncoder.default = datetime_handler

'''
for shard in response["StreamDescription"]["Shards"]:
    #print(shard);
    print( shard["ShardId"] + ": " + " StartKey:" + shard["HashKeyRange"]["StartingHashKey"]  + " End:" + shard["HashKeyRange"]["EndingHashKey"]  );
    print( -int(shard["HashKeyRange"]["StartingHashKey"]) + int(shard["HashKeyRange"]["EndingHashKey"]));
'''

