import boto3
import json

iot = boto3.client('iot')
sns_client = boto3.client('sns')

def push_to_sns(i, message):
    req = sns_client.publish_batch(
        TopicArn = 'arn:aws:sns:ap-south-1:776601892319:force_trigger_stats',
        PublishBatchRequestEntries = message
    )
    print("Req no ", i, " -> success :- ", len(req["Successful"]), " , failed :- ", len(req["Failed"]))

def create_part_push(messages, start_date, end_date):
    i = 0
    part = []
    for message in messages:
        if len(part) == 5:
            vin_imeis_req = {}
            vin_imeis_req["Id"] = part[0]
            vin_imeis_req["Message"] = json.dumps({
            "vin_imeis": ",".join(part),
            "start_date": start_date,
            "end_date": end_date
            })
            push_to_sns(i, [vin_imeis_req])
            i += 1
            part = []
        part.append(message)
    if len(part) > 0:
        vin_imeis_req = {}
        vin_imeis_req["Id"] = part[0]
        vin_imeis_req["Message"] = json.dumps({
            "vin_imeis": ",".join(part),
            "start_date": start_date,
            "end_date": end_date
            })
        push_to_sns(i, [vin_imeis_req])

def get_customers():
    print("Getting customers")
    continuation_token = ""
    not_done = True
    vin_imeis = []
    while not_done:
        if continuation_token == "":
            response = iot.list_things()
        else:
            response = iot.list_things(nextToken=continuation_token)
        not_done = 'nextToken' in response
        if not_done:
            continuation_token = response['nextToken']
        for thing in response['things']:
            if 'attributes' in thing:
                if 'env' in thing['attributes'] and thing['attributes']['env'].lower() == 'customer':
                    imei = thing['thingName'].split('_')[1]
                    vin = ""
                    if 'VIN_ID' in thing['attributes']:
                        vin = thing['attributes']['VIN_ID']
                        vin_imeis.append(vin.strip() + "-" + imei.strip())
    return vin_imeis

def lambda_handler(event, context):
    vin_imeis = get_customers()
    print("Customer count :- ", len(vin_imeis))
    start_date = event["start_date"]
    end_date = event["end_date"]
    create_part_push(vin_imeis[:2], start_date, end_date)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
