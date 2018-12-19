"C:\Program Files\WinRAR\winrar.exe" a -afzip function.zip *.py

aws lambda update-function-code --function-name hackweek-autoscale-kds --zip-file fileb://function.zip
{
    "FunctionName": "hackweek-autoscale-kds",
    "FunctionArn": "arn:aws:lambda:us-west-2:236573294224:function:hackweek-autoscale-kds",
    "Runtime": "python3.6",
    "Role": "arn:aws:iam::123456789012:role/grate-lambda-s3-execution-role",
    "Handler": "main.handler_function",
    "Description": "",
    "Timeout": 60,
    "MemorySize": 512,
    "LastModified": "2018-11-20T20:41:16.647+0000",
    "Version": "$LATEST",
    "RevisionId": "d1e983e3-ca8e-434b-8dc1-7add83d72ebd"
}