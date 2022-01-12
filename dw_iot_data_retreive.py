import boto3
import json
from botocore.vendored import requests

def lambda_handler(event, context):
    # TODO implement
    api_url = 'http://10.52.xx.xx/Thingworx/Things/CIMC.RestFulForCIMC.Helper/Services/GetAllMeterKWHRealTime'
    appKey = 'c9effe6e-fa55-46b2-b63e-xxxxxxxx'
    kinesis_firehose = 'cimc-iot-data-firehose'
    client = boto3.client('firehose')
    headers = {
    'appKey': appKey,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
    }
    try: 
        iot_response = requests.post(api_url, headers=headers)
        put_response = client.put_record(
            DeliveryStreamName=kinesis_firehose,
            Record={
            'Data': iot_response.text
            }
        )
        #print(put_response)
        return {
        'statusCode': 200,
        'body': json.dumps(iot_response.text)
        }
    except Exception as e:
        print(e)
    





