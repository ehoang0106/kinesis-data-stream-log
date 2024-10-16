import boto3
import gzip
import json
import io

s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')   

KINESIS_STREAM_NAME = 'BlogAnatylicsStream'

def lambda_function(event, context):
    # get s3 bucket and key from event
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    #get the log file from s3
    response = s3.get_object(Bucket=bucket, Key=key)
    log_content = response['Body'].read()
    
    #decompress the log file
    
    if key.endswith('.gz'):
        log_content = gzip.GzipFile(fileobj=io.BytesIO(log_content)).read()
        
    
    #parse the log file
    
    lines = log_content.decode('utf-8').strip().split('\n')
    for line in lines:
        if line.startswith('#'):
            continue
        
        # split the line into fields
        
        fields = line.split('\t')
        log_data = {
            "timestamp": fields[0],
            "request_ip": fields[4],
            "http_method": fields[5],
            "page_url": fields[7],
            "user_agent": fields[10],
        }
        
        #send data to Kinesis
        
        kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(log_data),
            PartitionKey=fields[4] # use the request_ip as the partition key
        )
        
        
    return { 'statusCode': 200, 'body': json.dumps('Log processed successfully') }