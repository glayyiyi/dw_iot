import boto3
import json
from botocore.vendored import requests
import psycopg2
import base64
from datetime import *

def lambda_handler(event, context):
    # TODO implement
    api_url = 'http://10.52.xx.xx/Thingworx/Things/CIMC.RestFulForCIMC.Helper/Services/GetAllMeterKWHRealTime'
    appKey = 'c9effe6e-fa55-46b2-b63e-xxxxxxxx'
    headers = {
    'appKey': appKey,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
    }
    try: 
        conn_string = "dbname='bigdatapocdb' port='5439' user='xxxxx' password='xxxxxxx' host='xxxxxxxx.cynwb8odrsdt.cn-north-1.redshift.amazonaws.com.cn'"
        batchtime=datetime.now()
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        iot_response = requests.post(api_url, headers=headers)
        iot_data=json.loads(iot_response.text)['rows']
        for record in iot_data:
            if 'workcentername' in record:
                workcentername=record['workcentername']
            else: 
                workcentername=''
            sql_string = "INSERT INTO  t_electricitymeterdegree_at_once (batchtime,name,description,realtimekwh,meterdisplayname,workcentername) VALUES (%s,%s,%s,%s,%s,%s);"
            cursor.execute(sql_string, (batchtime,record['name'], record['description'],record['RealtimeKWH'], record['MeterDisplayName'],workcentername))
        conn.commit()
        cursor.close()
        
        return {
        'statusCode': 200,
        'body': iot_response.text
        }
    except Exception as e:
        print(e)
