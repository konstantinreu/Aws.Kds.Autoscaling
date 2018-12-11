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
minShards = int(os.environ['SCALE_SHARDS_MIN'])
maxShards = int(os.environ['SCALE_SHARDS_MAX'])
targetUtilization = int(os.environ['SCALE_TARGET_UTILIZATION'])

#CwAlarmName = os.environ['AutoScaleKDS-Dev']
CwAlarmName = 'AutoScaleKDS-Dev'

def handler_function(event, context):
    KdsInfo = KdsWrapper.getKdsInfo(KdsName);
    print('KdsInfo ' + json.dumps(KdsInfo, default=datetime_handler));

    KdsLoadInfo = KdsWrapper.getKdsUtilization(KdsName)
    print('KdsLoad ' + json.dumps(KdsLoadInfo, default=datetime_handler));

    CloudwatchWrapper.publishMetrics('Scaling Metrics', KdsName, KdsLoadInfo['ShardCount'], KdsLoadInfo['Utilization'])

    if KdsLoadInfo['Utilization']  < 20 and KdsLoadInfo['ShardCount'] > minShards:
        print('Merging shards')
        KdsWrapper.mergeShards(KdsName,  KdsLoadInfo['ShardCount'], KdsLoadInfo['Utilization'], targetUtilization)
    elif KdsLoadInfo['Utilization'] > 60 and KdsLoadInfo['ShardCount'] < maxShards:
        print('Splitting shards')
    else:
        print('No actions')

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

