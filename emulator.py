from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json
import time
import csv
import json
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

MAX_ROWS = 50
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT")
vehicle_ids = [0, 1, 2, 3, 4]


class MQTTClient:
    def __init__(self, device_id, cert, key, subscribe_topic, publish_topic):
        self.device_id = device_id
        self.subscribe_topic = subscribe_topic
        self.publish_topic = publish_topic
        self.is_message_received = True

        # Create and configure MTTQ Client
        self.mqtt_client = AWSIoTMQTTClient(f"MyEmulatedDevice_{device_id}")
        self.mqtt_client.configureEndpoint(AWS_ENDPOINT, 8883)
        self.mqtt_client.configureCredentials(
            "./certificates/AmazonRootCA1.pem", key, cert
        )
        self.mqtt_client.configureAutoReconnectBackoffTime(1, 32, 20)
        self.mqtt_client.configureOfflinePublishQueueing(
            -1
        )  # Infinite offline publish queueing
        self.mqtt_client.configureDrainingFrequency(2)  # Draining frequency: 2 Hz
        self.mqtt_client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.mqtt_client.configureMQTTOperationTimeout(5)  # 5 sec

    def message_callback(self, client, userdata, message):
        print(
            "Received message from topic '{}': {}".format(
                message.topic, message.payload.decode("utf-8")
            )
        )
        self.set_is_message_received(True)

    def connect_to_client(self):
        self.mqtt_client.connect()
        self.set_is_message_received(False)
        # Subscribe to device topic
        self.mqtt_client.subscribe(self.subscribe_topic, 1, self.message_callback)
        print(f"Client connected and subscribed to topic {self.subscribe_topic}")

    def disconnect(self):
        self.mqtt_client.disconnect()

    def publish_data(
        self,
        payload,
    ):
        self.mqtt_client.publish(self.publish_topic, payload, 1)
        print("Published: " + payload)

    def get_is_message_received(self):
        return self.is_message_received

    def set_is_message_received(self, value):
        self.is_message_received = value


def csv_to_json_payload(csv_file_path):
    data = []

    with open(csv_file_path, mode="r", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        row_num = 0
        for row in csv_reader:
            if row_num < MAX_ROWS:
                data.append(row)
                row_num += 1
            else:
                break

    return json.dumps(data)


# Run program
clients = []
payload_by_id = {}
for v_id in vehicle_ids:
    key_path = f"./certificates/private_thing_{v_id}.key"
    cert_path = f"./certificates/cert_thing_{v_id}.pem"
    subscribe_topic = f"my/emulated/device_veh{v_id}"
    publish_topic = "my/devices/data"

    csv_file_path = f"./dataset/data2/vehicle{v_id}.csv"
    payload_by_id[v_id] = csv_to_json_payload(csv_file_path)

    client = MQTTClient(v_id, cert_path, key_path, subscribe_topic, publish_topic)
    client.connect_to_client()
    clients.append(client)


def is_message_received_all_clients(clients):
    for client in clients:
        if client.get_is_message_received() is False:
            print("Clients still waiting...")
            return False
    print("All devices have received a message")
    return True


messages_sent = False
while not is_message_received_all_clients(clients):
    if not messages_sent:
        for v_id, c in zip(vehicle_ids, clients):
            c.publish_data(payload_by_id.get(v_id, ""))
            time.sleep(3)
        print("Devices data sent")
        messages_sent = True
    print("Waiting for server response....")
    time.sleep(5)


for client in clients:
    client.disconnect()
exit()
