import boto3
import CloudwatchWrapper
import json
import datetime

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

# Merge shards to reach target utilization
def mergeShards(KdsName, shardCountNow, currentUtilization, targetUtilization):
    targetReducePct = 0;
    targetShard = 0;

    if currentUtilization == 0:
        targetReducePct = 100;
    else:
        targetReducePct =  (currentUtilization - targetUtilization) / targetUtilization;

    #post correction
    if targetReducePct > 90:
        targetReducePct = 75;

    targetShard = shardCountNow - int(shardCountNow * targetReducePct / 100)

    print("targetReducePct " + str(targetReducePct) + ". targetShards " + str(targetShard))

    #get opened shards. Sort by key ranges. Take Top N by smallest range. Merge them

    return


def splitShards(KdsName):
    return


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