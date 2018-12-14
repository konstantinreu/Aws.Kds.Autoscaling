import json
import sys
import traceback
import boto3
import datetime
import os
import Utils
import CloudwatchWrapper
import KdsWrapper
import math


# initialize client
KdsClient = boto3.client('kinesis');
CwClient = boto3.client('cloudwatch');
KdsArn = os.environ['AWS_KINESIS_STREAM_ARN']
KdsName = Utils.parse_arn(KdsArn)['resource'];
ScaleMinShards = int(os.environ['SCALE_SHARDS_MIN'])
ScaleMaxShards = int(os.environ['SCALE_SHARDS_MAX'])
TargetUtilizationPct = int(os.environ['SCALE_TARGET_UTILIZATION'])
MaxIncreasePct = 80;     # 100 - most aggresive number, means double shards or reduce twice per run
MaxReducePct   = 30;     # 100 - most aggresive number, means double shards or reduce twice per run

#CwAlarmName = os.environ['AutoScaleKDS-Dev']
CwAlarmName = 'AutoScaleKDS-Dev'

def handler_function(event, context):
    KdsInfo = KdsWrapper.getKdsInfo(KdsName);
    #print('KdsInfo ' + json.dumps(KdsInfo, default=datetime_handler));

    KdsLoadInfo = KdsWrapper.getKdsUtilization(KdsName)
    print('KdsLoad ' + json.dumps(KdsLoadInfo, default=datetime_handler));

    #ActiveShards list, pre-sorted by StartingHashKey
    ActiveShards =  sorted(
                            filter(lambda x: 'EndingSequenceNumber' not in x['SequenceNumberRange'], KdsInfo['Shards']),
                            key=lambda x: int(x['HashKeyRange']['StartingHashKey'])
                    );
    #print(json.dumps(ActiveShards, default=datetime_handler));
    #logging of current metrics

    CurrentUtilizationPct = KdsLoadInfo['Utilization'];
    TargetReducePct = 0;
    TargetIncreasePct = 0;
    CurrentShardsCount = int(len(ActiveShards));
    TargetShardsCount  = int(len(ActiveShards));

    #CurrentUtilizationPct = 80
    #CurrentShardsCount = 10

    CloudwatchWrapper.publishMetrics('Scaling Metrics', KdsName, CurrentShardsCount, CurrentUtilizationPct)
    CloudwatchWrapper.putLog(
        'Stats: {KdsName}. Utilization: {Utilization}, ShardsCount {TotalShardsCount} , ActiveShardsCount {ActiveShardsCount}'.format(
                KdsName = KdsName,
                Utilization = CurrentUtilizationPct,
                TotalShardsCount = len( KdsInfo['Shards'] ),
                ActiveShardsCount = CurrentShardsCount
        ), False );

    # Determine action and targetnumber of shards
    # Ranges should be targetUtiliazation +- 10%
    if CurrentUtilizationPct < (TargetUtilizationPct - 5) and CurrentShardsCount > ScaleMinShards:
        if CurrentUtilizationPct == 0:
            TargetReducePct = MaxReducePct;
        else:
            TargetReducePct = 100*(TargetUtilizationPct - CurrentUtilizationPct ) / CurrentUtilizationPct;

        # post correction
        if TargetReducePct > MaxReducePct:
            TargetReducePct = MaxReducePct;


        TargetShardsCount = int(CurrentShardsCount * (1-TargetReducePct/100));
        CloudwatchWrapper.putLog( "Action: MERGE. TargetReducePct " + str(TargetReducePct) + "%. TargetShards " + str(TargetShardsCount), False);

        KdsWrapper.mergeShards(KdsName, ActiveShards, KdsInfo, CurrentShardsCount, TargetShardsCount);
    elif CurrentUtilizationPct > (TargetUtilizationPct + 5) and CurrentShardsCount < ScaleMaxShards:
        TargetIncreasePct = 100*(CurrentUtilizationPct - TargetUtilizationPct) / TargetUtilizationPct;

        # post correction
        if TargetIncreasePct > MaxIncreasePct:
            TargetIncreasePct = MaxIncreasePct;

        TargetShardsCount =  math.ceil(CurrentShardsCount * (1+TargetIncreasePct/100));

        print("Action: SPLIT. TargetIncreasePct " + str(TargetIncreasePct) + "%. TargetShards " + str(TargetShardsCount))
        KdsWrapper.splitShards(KdsName, ActiveShards, CurrentShardsCount, TargetShardsCount)
    else:
        print('No actions')

    # condition for OPENED shards
    '''

    print( json.dumps(
        KdsClient.list_shards(
        StreamName='rdm-dev-intake-kds-main',
        #NextToken='string',
        #ExclusiveStartShardId='string',
        MaxResults=100
       # StreamCreationTimestamp=datetime(2015, 1, 1)
    ), default=datetime_handler ));
    '''
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

