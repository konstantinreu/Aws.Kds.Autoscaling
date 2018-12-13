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
        Limit=100
    );

    resp = {
        'StreamStatus'  : responseSummary['StreamDescriptionSummary']['StreamStatus'],
        'OpenShardCount': responseSummary['StreamDescriptionSummary']['OpenShardCount'],
        'Shards'        : responseDetails["StreamDescription"]["Shards"]
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

    if len(rawMetric['Values']) > 0:
        currentThroughputKbps = rawMetric['Values'][0] / 1048576/period;
        currentTS = rawMetric['Timetamps'][0];


    return {
                   'ShardCount'         : openShards,
                   'ThroughPutActual'   : currentThroughputKbps,
                   'Utilization'        : 100* currentThroughputKbps / openShards,
                   'ThroughPutMax'      : openShards,
                   'ValidOn'            : currentTS
    };
#############################################
# MERGE shards to reach target utilization
#############################################
def mergeShards(KdsName, ActiveShards, KdsInfo, CurrentShardsCount, TargetShardsCount):
    # Check
    NumOfMerges = CurrentShardsCount - TargetShardsCount;
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


    #Sort shards desc by distance HiKey - LowKey
    ShardPairCandidates = sorted(
                                ShardPairCandidates,
                                key = lambda x: int( x['CombinedKeyRange']),
                                reverse=1);

    # To reach TargetShardsCount, we have to perform NumOfSplits splitting
    print(json.dumps(ShardPairCandidates));
    for idx, pair in  enumerate(ShardPairCandidates[:NumOfMerges]):
        CloudwatchWrapper.putLog('MERGE # ' + str(idx) + ' Shard1: ' + pair['ShardId1'] + ' & Shard2: ' + pair['ShardId2'], False);

        KdsClient.merge_shards(
            StreamName   = KdsName,
            ShardToMerge = pair['ShardId1'],
            AdjacentShardToMerge=pair['ShardId2']
        );
        # wait until split finishes. Queue status should be Active
        waitActiveState4KDS;

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
    for x in ShardsSortedByRangePool[:NumOfSplits]:
        newStartingHashKey = ((int(x['HashKeyRange']['StartingHashKey'])+int(x['HashKeyRange']['EndingHashKey']))/2)
        KdsClient.split_shard(
            StreamName=KdsName,
            ShardToSplit=x['ShardId'],
            NewStartingHashKey=str(newStartingHashKey)
        );
        # wait until split finishes. Queue status should be Active
        waitActiveState4KDS;

    return


def waitActiveState4KDS(KdsName):
    timeoutSec = 3;
    sleepIntervalSec = 3;
    intervalWaited = 0;

    while True:
        time.sleep(sleepIntervalSec);
        if getKdsInfo(KdsName)['StreamStatus'] == 'ACTIVE':
            break
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