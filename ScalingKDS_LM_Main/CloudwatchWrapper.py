import boto3
import datetime

CwClient = boto3.client('cloudwatch');

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
                'Unit': 'None'
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

'''
Metric{"MetricDataResults": [{"Id": "idIncomingBytes", "Label": "IncomingBytes", "Timestamps": [], "Values": [], "StatusCode": "Complete"}], "ResponseMetadata": {"RequestId": "a4a5a8e1-fd93-11e8-a502-8907912142bb", "HTTPStatusCode": 200, "HTTPHeaders": {"x-amzn-requestid": "a4a5a8e1-fd93-11e8-a502-8907912142bb", "content-type": "text/xml", "content-length": "493", "date": "Tue, 11 Dec 2018 22:25:19 GMT"}, "RetryAttempts": 0}}
'''