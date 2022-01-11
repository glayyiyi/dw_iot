import base64
import json

def lambda_handler(event, context):
    output = []
    payload=""
    for record in event['records']:
        print(record['recordId'])
        rawData = base64.b64decode(record['data'])
        print(rawData)
        
        # Do custom processing on the payload here
        for row in json.loads(rawData)['rows']:
            payload+=json.dumps(row)
            
        print(payload)
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(payload)
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}