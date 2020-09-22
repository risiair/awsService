from __future__ import print_function
from flask import Flask, request, abort, Response, jsonify
import json


app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

@app.route("/read_items", methods=['POST'])
def read_items():
    from botocore.config import Config
    from boto3.dynamodb.conditions import Key
    import boto3
    import json
    import decimal
    
    #event = json.loads(event['body'])
    body = request.get_data(as_text=True)
    event = json.loads(body)

    
    #declare table instance
    dynamodb = boto3.resource('dynamodb',  region_name='ap-northeast-1')
    
    TableName = event['table']
    table = dynamodb.Table(TableName)
    key1 = event['payload']['key1']

    if(TableName == 'UserActivity'):
        primary_key1 = 'DeviceID_UserID'
        primary_key2 = 'OrderTimestamp'
        key2range = event['payload']['key2range']
        
        response = table.query(
        KeyConditionExpression= Key(primary_key1).eq(key1)\
        & Key(primary_key2).between(key2range[0], key2range[1]))
    elif(TableName == 'UserInfo'):
        primary_key1 = 'DeviceID_UserID'
        response = table.query(KeyConditionExpression= Key(primary_key1).eq(key1))
    else:
        responese = {
            'statusCode': 777,
            'headers': {'Content-Type': 'application/json'},
            'body': "no such table"
        }
        return jsonify(responese)

    class DecimalEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, decimal.Decimal):
                return int(o)
            return super(DecimalEncoder, self).default(o)
    
    response = {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        #'body': json.dumps(response['Items'], cls=DecimalEncoder)
        'body': response['Items']
    }
    return jsonify(response)


@app.route("/write_items", methods=['POST'])
def write_items():
    from botocore.config import Config
    from boto3.dynamodb.conditions import Key
    import boto3
    import json
    

    body = request.get_data(as_text=True)
    event = json.loads(body)

    #event = json.loads(event['body'])
    
    #declare table instance
    dynamodb = boto3.resource('dynamodb',  region_name='ap-northeast-1')
    TableName = event['table']
    table = dynamodb.Table(TableName)
        
    with table.batch_writer() as batch:
        for item in event['payload']:
            batch.put_item(Item = item)
    response = {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': 'create success'
    }
    return jsonify(response)

    

if __name__ == '__main__':
    app.run(debug=True)