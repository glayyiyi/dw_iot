import json
import urllib.parse
import boto3
import psycopg2
print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        print(key)
        conn_string = "dbname='bigdatapocdb' port='5439' user='root' password='Test12345' host='bigdatapoc.cynwb8odrsdt.cn-north-1.redshift.amazonaws.com.cn'"
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("copy t_electricitymeterdegree(name,description,realtimekwh,meterdisplayname,workcentername) from '"+"s3://dwpocbucket/"+key+"' iam_role 'arn:aws-cn:iam::675378736534:role/MyRedshiftRole' format as json 's3://dwpocbucket/testJSONDianbiao_patch.json';")
        cursor.execute("call dataprocess()")
        conn.commit()
        cursor.close()
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
