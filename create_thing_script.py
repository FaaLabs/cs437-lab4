import os
import boto3
import json
import random
import string
from dotenv import load_dotenv
import os

POLICY_NAME = "My_Iot_Policy"

# Load .env file
load_dotenv()

def create_thing(thing_number, thing_name):
    thing_arn = ""
    global thingClient
    thing_response = thingClient.create_thing(thingName=thing_name)
    data = json.loads(json.dumps(thing_response, sort_keys=False, indent=4))
    for element in data:
        if element == "thingArn":
            thing_arn = data["thingArn"]
        create_certificate(thing_number, thing_name, thing_arn)


def create_certificate(thing_number, thing_name, thing_arn):
    global thingClient
    cert_response = thingClient.create_keys_and_certificate(setAsActive=True)
    data = json.loads(json.dumps(cert_response, sort_keys=False, indent=4))
    for element in data:
        if element == "certificateArn":
            certificate_arn = data["certificateArn"]
        elif element == "keyPair":
            public_key = data["keyPair"]["PublicKey"]
            private_key = data["keyPair"]["PrivateKey"]
        elif element == "certificatePem":
            certificate_pem = data["certificatePem"]

    with open(f"./certificates/public_thing_{thing_number}.key", "w") as outfile:
        outfile.write(public_key)
    with open(f"./certificates/private_thing_{thing_number}.key", "w") as outfile:
        outfile.write(private_key)
    with open(f"./certificates/cert_thing_{thing_number}.pem", "w") as outfile:
        outfile.write(certificate_pem)

    thingClient.attach_policy(policyName=POLICY_NAME, target=certificate_arn)
    thingClient.attach_thing_principal(
        thingName=thing_name, principal=certificate_arn
    )
    thingClient.add_thing_to_thing_group(
        thingGroupName="My_thing_group",
        thingGroupArn="arn:aws:iot:us-east-2:941377129615:thinggroup/My_thing_group",
        thingName=thing_name,
        thingArn=thing_arn,
        overrideDynamicGroups=False,
    )


thingClient = boto3.client(
    "iot",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-2",
)

for thing_number in range(5):
    thing_name = "".join(
        [random.choice(string.ascii_letters + string.digits) for _ in range(15)]
    )
    create_thing(thing_number, thing_name)
