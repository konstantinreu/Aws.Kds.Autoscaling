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
ScaleMinShards = int(os.environ['SCALE_SHARDS_MIN'])
ScaleMaxShards = int(os.environ['SCALE_SHARDS_MAX'])
TargetUtilizationPct = int(os.environ['SCALE_TARGET_UTILIZATION'])
MaxReduceIncreasePct = 50;     # 100 - most aggresive number, means double shards or reduce twice per run

#CwAlarmName = os.environ['AutoScaleKDS-Dev']
CwAlarmName = 'AutoScaleKDS-Dev'

def handler_function(event, context):
    KdsInfo = KdsWrapper.getKdsInfo(KdsName);
    print('KdsInfo ' + json.dumps(KdsInfo, default=datetime_handler));

    KdsLoadInfo = KdsWrapper.getKdsUtilization(KdsName)
    print('KdsLoad ' + json.dumps(KdsLoadInfo, default=datetime_handler));

    #ActiveShards list, pre-sorted by StartingHashKey
    ActiveShards =  sorted(
                            filter(lambda x: 'EndingSequenceNumber' not in x['SequenceNumberRange'], KdsInfo['Shards']),
                            key=lambda x: x['HashKeyRange']['StartingHashKey']
                    );

    #logging of current metrics
    CloudwatchWrapper.publishMetrics('Scaling Metrics', KdsName, KdsLoadInfo['ShardCount'], KdsLoadInfo['Utilization'])
    CloudwatchWrapper.putLog(
        'KDS: {KdsName}. Utilization: {Utilization}, ShardsCount {TotalShardsCount} , ActiveShardsCount {ActiveShardsCount}'.format(
                KdsName = KdsName,
                Utilization = KdsLoadInfo['Utilization'],
                TotalShardsCount = len( KdsInfo['Shards'] ),
                ActiveShardsCount = len(ActiveShards)
        ), False );


    CurrentUtilization =  20 #int(KdsLoadInfo['Utilization']),
    TargetReducePct = 0;
    TargetIncreasePct = 0;
    CurrentShardsCount = int(len(ActiveShards));
    TargetShardsCount  = int(len(ActiveShards));

    # Determine action and targetnumber of shards
    # Ranges should be targetUtiliazation +- 10%
    if CurrentUtilization < 40 and CurrentShardsCount > ScaleMinShards:
        if CurrentUtilization == 0:
            TargetReducePct = MaxReduceIncreasePct;
        else:
            TargetReducePct = 100*(TargetUtilizationPct - CurrentUtilization ) / CurrentUtilization;

        TargetShardsCount = int(CurrentShardsCount *  TargetReducePct/100);
        print("Action: MERGE. TargetReducePct " + str(TargetReducePct) + "%. TargetShards " + str(TargetShardsCount))

        KdsWrapper.mergeShards(KdsName, ActiveShards, KdsInfo, CurrentShardsCount, TargetShardsCount);
    elif KdsLoadInfo['Utilization'] > 60 and CurrentShardsCount < ScaleMaxShards:
        TargetIncreasePct = 100*(CurrentUtilization - TargetUtilizationPct) / CurrentUtilization;

        # post correction
        if TargetIncreasePct > MaxReduceIncreasePct:
            TargetIncreasePct = MaxReduceIncreasePct;

        TargetShardsCount = int(CurrentShardsCount * (1+TargetIncreasePct/100));

        #KdsWrapper.splitShards(KdsName, ActiveShards, KdsInfo,  )
        print("Action: MERGE. TargetReducePct " + str(TargetReducePct) + "%. TargetShards " + str(TargetShardsCount))
        #splitShards(KdsName, ActiveShards, KdsInfo, CurrentShardsCount, TargetShardsCount):
    else:
        print('No actions')

    # condition for OPENED shards
    '''
    shardsMergeCandidates = []
    for shard in sorted(  filter(lambda x:  'EndingSequenceNumber' not in x['SequenceNumberRange'],  KdsInfo['Shards'] ), key=lambda x: x['HashKeyRange']['StartingHashKey'] ):
        shardsMergeCandidates.append(
            {
                'ShardID1'  : shard['ShardId']
                #'ShardID2'  : shard['ShardId'],
            }
        );

        print(json.dumps(shard))
    '''
    print( json.dumps(
        KdsClient.list_shards(
        StreamName='rdm-dev-intake-kds-main',
        #NextToken='string',
        #ExclusiveStartShardId='string',
        MaxResults=100
       # StreamCreationTimestamp=datetime(2015, 1, 1)
    ), default=datetime_handler ));
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

