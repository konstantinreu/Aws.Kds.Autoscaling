import boto3
import datetime
import time
import json

CwClient = boto3.client('cloudwatch');
LogsClient = boto3.client('logs')
LogsSequenceToken = "";

def publishMetrics(dimensionName, dimensionValue, shardCount, utilizationPct):
    CwClient.put_metric_data(
        Namespace='Kinesis',
        MetricData=[
            # Shard Count
            {
                'MetricName': 'ShardCount',
                'Dimensions': [
                    {
                        'Name':  dimensionName,
                        'Value': dimensionValue
                    }
                ],
                #'Timestamp': datetime(2015, 1, 1),
                'Value': shardCount,
                'Unit':  'None'
                #,'StorageResolution': 123
            },
            # Utilization
            {
                'MetricName': 'UtilizationPct',
                'Dimensions': [
                    {
                        'Name':  dimensionName,
                        'Value': dimensionValue
                    }
                ],
                #'Timestamp': datetime(2015, 1, 1),
                'Value': utilizationPct,
                'Unit': 'Percent'
                # ,'StorageResolution': 123
            },

        ]
    )

def retrieveMetric(nameSpace, dimensionName, dimensionValue, metricName, period=60, lookbackRangeSec = 300):
    response = CwClient.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'id' + metricName,
                'MetricStat': {
                    'Metric': {
                        'Namespace': nameSpace,
                        'MetricName': metricName,
                        'Dimensions': [
                            {
                                'Name': dimensionName,
                                'Value': dimensionValue
                            },
                        ]
                    },
                    'Period': period,
                    'Stat': 'Sum'
                   # 'Unit': 'Count'
                },
                #'Expression': 'string',
                #'Label': 'string',
                'ReturnData': True
            },
        ],
        StartTime=datetime.datetime.utcnow() - datetime.timedelta(seconds=lookbackRangeSec),   #last 5 minutes
        EndTime=datetime.datetime.utcnow(),
        #NextToken='string',
        ScanBy='TimestampDescending',
        MaxDatapoints=100
    )
    return response;



def updateAlarm(alarmName, crShardCount):
    CwClient.put_metric_alarm(
            AlarmName=alarmName,
            AlarmDescription=alarmName,
            ActionsEnabled = True,
            AlarmActions=['arn:aws:sns:us-west-2:236573294224:rdm-dev-scale-kds-alerts'],
            MetricName='IncomingBytes',
            Namespace='AWS/Kinesis',
            Statistic='Sum',
            #ExtendedStatistic='string',
            Dimensions=[
                {
                    'Name': 'StreamName',
                    'Value': 'rdm-dev-intake-kds-main'
                },
            ],
            Period=60,
            Unit='Seconds',
            EvaluationPeriods=1,
            #DatapointsToAlarm=123,
            Threshold= crShardCount * 800 * 1024,
            ComparisonOperator='GreaterThanOrEqualToThreshold',
            TreatMissingData='missing'
            #EvaluateLowSampleCountPercentile='string',
    );

def putLog(message, isError, redirectToConsole = True):
    # we need token for .put_log_events
    global LogsSequenceToken;

    if redirectToConsole == True:
        print(message);

    if LogsSequenceToken == "":
        response = LogsClient.describe_log_streams(
            logGroupName='rdm/MainGroup',
            logStreamNamePrefix='KdsScaleLogStream',
            orderBy='LogStreamName',
            descending=True
        );

        if 'uploadSequenceToken' in response['logStreams'][0]:
            LogsSequenceToken = response['logStreams'][0]['uploadSequenceToken']
        #print('LogsSequenceToken: ' + LogsSequenceToken + '. Dump: ' +json.dumps(response));

    resp = LogsClient.put_log_events(
        logGroupName  = 'rdm/MainGroup',
        logStreamName = 'KdsScaleLogStream',
        logEvents=[
            {
                'timestamp': int(round(time.time() * 1000)),
                'message': message
            }
        ],
     sequenceToken = LogsSequenceToken
    )

    LogsSequenceToken = resp['nextSequenceToken'];


'''
Metric{"MetricDataResults": [{"Id": "idIncomingBytes", "Label": "IncomingBytes", "Timestamps": [], "Values": [], "StatusCode": "Complete"}], "ResponseMetadata": {"RequestId": "a4a5a8e1-fd93-11e8-a502-8907912142bb", "HTTPStatusCode": 200, "HTTPHeaders": {"x-amzn-requestid": "a4a5a8e1-fd93-11e8-a502-8907912142bb", "content-type": "text/xml", "content-length": "493", "date": "Tue, 11 Dec 2018 22:25:19 GMT"}, "RetryAttempts": 0}}
'''