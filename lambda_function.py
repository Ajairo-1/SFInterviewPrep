import json

def lambda_handler(event, context):
    print(f"Event body is: {event['body']}")
    telemetry_dict = json.loads(event['body'])
    # the incoming shape changes based on what's calling the function
    # 2 options of what's returned:
    #   - Manual test: raw dict
    #   - API Gateway: envelope of statusCode and body, where body=JSON_String
    return {
        "statusCode": 200,
        "body": json.dumps(telemetry_dict)
    }
    #json.loads()-> deserialize _ to Python_ob
    #json.dumps()-> serialize obj to JSON formatted String
