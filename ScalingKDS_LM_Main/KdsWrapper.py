import boto3
import CloudwatchWrapper
import json
import datetime
import time
import sys

KdsClient = boto3.client('kinesis');

def getKdsInfo(KdsName):
    responseSummary = KdsClient.describe_stream_summary(
        StreamName=KdsName
    );

    responseDetails = KdsClient.describe_stream(
        StreamName=KdsName,
        Limit=100);

    shards = responseDetails["StreamDescription"]["Shards"]

    while responseDetails['StreamDescription']['HasMoreShards'] == True:
        StartShardID = responseDetails["StreamDescription"]["Shards"][-1]['ShardId']
        responseDetails = KdsClient.describe_stream(
            StreamName=KdsName,
            ExclusiveStartShardId = StartShardID,
            Limit=100);
        shards.extend( responseDetails["StreamDescription"]["Shards"]);


    resp = {
        'StreamStatus'  : responseSummary['StreamDescriptionSummary']['StreamStatus'],
        'OpenShardCount': responseSummary['StreamDescriptionSummary']['OpenShardCount'],
        'Shards'        : shards
    }

    return resp;


def getKdsUtilization(KdsName, lookbackRangeSec = 60):
    period = 60

    rawMetric = CloudwatchWrapper.retrieveMetric(
        nameSpace     ='AWS/Kinesis',
        dimensionName ='StreamName',
        dimensionValue=KdsName,
        metricName       ='IncomingBytes',
        period           = period,
        lookbackRangeSec = lookbackRangeSec
    )['MetricDataResults'][0];

    openShards = getKdsInfo(KdsName)['OpenShardCount']
    currentThroughputKbps = 0;
    currentTS = datetime.datetime.utcnow();

    #print(json.dumps(rawMetric))
    if len(rawMetric['Values']) > 0:
        currentThroughputKbps = rawMetric['Values'][0] / 1048576/period;
        currentTS = rawMetric['Timestamps'][0];


    return {
                   'ShardCount'         : openShards,
                   'ThroughPutActual'   : currentThroughputKbps,
                   'Utilization'        : int(100* currentThroughputKbps / openShards),
                   'ThroughPutMax'      : openShards,
                   'ValidOn'            : currentTS
    };
#############################################
# MERGE shards to reach target utilization
#############################################
def mergeShards(KdsName, ActiveShards, KdsInfo, CurrentShardsCount, TargetShardsCount):
    # Check
    NumOfMerges = int(CurrentShardsCount - TargetShardsCount);
    if NumOfMerges < 0:
        raise Exception('TargetShardsCount cannot be greater than CurrentShardsCount for merging operation !')

    #ActiveShards already sorted, so two adjacent shards are candidates
    ShardPairCandidates = [];
    for idx, shard in  enumerate(ActiveShards[:-1]):
        nextShard = ActiveShards[idx+1];
        ShardPairCandidates.append(
            {
                'ShardId1'          : shard['ShardId'],
                'ShardId2'          : nextShard['ShardId'],
                'ShardId1KeyRanges' : shard['HashKeyRange'],
                'ShardId2KeyRanges' : nextShard['HashKeyRange'],
                'CombinedKeyRange'  : int(int(shard['HashKeyRange']['StartingHashKey'])+int(nextShard['HashKeyRange']['EndingHashKey']))
            }
        )

    # Perform number of merges
    for i in range(0, NumOfMerges):
        # Sort shards desc by distance HiKey - LowKey
        ShardPairCandidates = sorted(
                                    ShardPairCandidates,
                                    key = lambda x: int( x['CombinedKeyRange']),
                                    reverse=0);
        print(json.dumps(ShardPairCandidates))
        pair = ShardPairCandidates[0];
        # wait until split finishes. Queue status should be Active
        waitActiveState4KDS(KdsName);
        CloudwatchWrapper.putLog('MERGE # ' + str(i) + ' Shard1: ' + pair['ShardId1'] + ' & Shard2: ' + pair['ShardId2'], False);
        KdsClient.merge_shards(
            StreamName   = KdsName,
            ShardToMerge = pair['ShardId1'],
            AdjacentShardToMerge=pair['ShardId2']
        );

        #adjust leftover pairs in array
        ShardPairCandidates = list(filter(lambda x: not (x['ShardId1'] == pair['ShardId2'] or x['ShardId1'] == pair['ShardId1'] or x['ShardId2'] == pair['ShardId1'] or x['ShardId2'] == pair['ShardId2']  ), ShardPairCandidates))
        print('ShardPairCandidates left :' + str(len(ShardPairCandidates)));

        if len(ShardPairCandidates) == 0:
            print('No more candidates for merge!')
            break;


    #get opened shards. Sort by key ranges. Take Top N by smallest range. Merge them

    return

#############################################
# SPLIT shards to reach target utilization
#############################################
def splitShards(KdsName, ActiveShards, CurrentShardsCount, TargetShardsCount):
    # Check
    NumOfSplits = TargetShardsCount - CurrentShardsCount;
    if NumOfSplits < 0:
        raise Exception('TargetShardsCount cannot be less than CurrentShardsCount for splitting operation !')

    #Sort shards desc by distance HiKey - LowKey
    ShardsSortedByRangePool = sorted(
                                    filter(lambda x:  'EndingSequenceNumber' not in x['SequenceNumberRange'],  ActiveShards),
                                    key =lambda x: (int(x['HashKeyRange']['EndingHashKey'])-int(x['HashKeyRange']['StartingHashKey'])),
                                    reverse=1)

    # To reach TargetShardsCount, we have to perform NumOfSplits splitting
    for idx, x in enumerate(ShardsSortedByRangePool[:NumOfSplits]):
        waitActiveState4KDS(KdsName);
        newStartingHashKey = int( (int(x['HashKeyRange']['StartingHashKey'])+int(x['HashKeyRange']['EndingHashKey']))/2 )
        CloudwatchWrapper.putLog('SPLIT # ' + str(idx) + ' Shard1: ' + x['ShardId'], False);

        KdsClient.split_shard(
            StreamName=KdsName,
            ShardToSplit=x['ShardId'],
            NewStartingHashKey=str(newStartingHashKey)
        );

        # wait until split finishes. Queue status should be Active

    return


def waitActiveState4KDS(KdsName):
    timeoutSec = 30;
    sleepIntervalSec = 1;
    intervalWaited = 0;

    while True:
        #print(getKdsInfo(KdsName)['StreamStatus'])
        if getKdsInfo(KdsName)['StreamStatus'] == 'ACTIVE':
            break
        time.sleep(sleepIntervalSec);
        intervalWaited+=1;
        if intervalWaited > (timeoutSec / sleepIntervalSec):
            raise Exception('Timeout for waiting ACTIVE status of KDS:' + KdsName)



'''

Summary: {"StreamDescriptionSummary": {"StreamName": "rdm-dev-intake-kds-main",
                                       "StreamARN": "arn:aws:kinesis:us-west-2:236573294224:stream/rdm-dev-intake-kds-main",
                                       "StreamStatus": "ACTIVE", "RetentionPeriodHours": 24,
                                       "StreamCreationTimestamp": "2018-05-02T10:29:35-07:00",
                                       "EnhancedMonitoring": [{"ShardLevelMetrics": ["IncomingBytes",
                                                                                     "OutgoingRecords",
                                                                                     "IteratorAgeMilliseconds",
                                                                                     "IncomingRecords",
                                                                                     "ReadProvisionedThroughputExceeded",
                                                                                     "WriteProvisionedThroughputExceeded",
                                                                                     "OutgoingBytes"]}],
                                       "EncryptionType": "NONE", "OpenShardCount": 5, "ConsumerCount": 0},
          "ResponseMetadata": {"RequestId": "c97927d3-da18-403f-9df3-2749dd1328a8", "HTTPStatusCode": 200,
                               "HTTPHeaders": {"x-amzn-requestid": "c97927d3-da18-403f-9df3-2749dd1328a8",
                                               "x-amz-id-2": "cwe0EixBNYyfRyC05HGKL0sVs1PGIfOvaDCiOqt1;gr5PKxBWHz2+sY3O05SoRPocvK/bx6gqeVkrAZFn05ujaj0NRPrAUm",
                                               "date": "Tue, 11 Dec 2018 18:13:46 GMT",
                                               "content-type": "application/x-amz-json-1.1",
                                               "content-length": "518"}, "RetryAttempts": 0}}
'''