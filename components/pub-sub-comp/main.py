import threading
import traceback

import awsiot.greengrasscoreipc.clientv2 as clientV2
import json
import boto3

subscribe_topic = "my/devices/data"
publish_topic_prefix = "my/emulated/device_"
qos = "1"


def send_to_firehose(records):
    stream_name = "PUT-S3-Bwc2R"
    firehose_client = boto3.client("firehose")

    # Prepare the records for Firehose
    firehose_records = [{"Data": json.dumps(record)} for record in records]

    try:
        # Send the records to Firehose
        response = firehose_client.put_record_batch(
            DeliveryStreamName=stream_name, Records=firehose_records
        )

        # Output the response
        print("Firehose response:", response)
    except Exception as e:
        print("Failed to send records to Firehose:", e)
    finally:
        firehose_client.close()

def get_max_co2(message):
    max_counter = 0.0
    for record in message:
        co2_val = float(record["vehicle_CO2"])
        vehicle_stat = record["vehicle_id"]

        if co2_val > max_counter:
            max_counter = co2_val

    return vehicle_stat, max_counter


def on_stream_event(event):
    try:
        message = json.loads(event.message.payload)
        print(f"Received new message on topic {subscribe_topic}.")

        vehicle_stat, max_counter = get_max_co2(message)
        publish_topic_name = publish_topic_prefix + vehicle_stat

        print("Sending max CO2 result back.")
        ipc_client.publish_to_iot_core(
            topic_name=publish_topic_name,
            qos=qos,
            payload=json.dumps(
                {
                    "max_CO2": max_counter,
                }
            ),
        )

        print("Sending received message records to firehose.")
        send_to_firehose(message)

        print("Event process was successful.")
    except:
        traceback.print_exc()


def on_stream_error(error):
    # Return True to close stream, False to keep stream open.
    print("There was an error on the stream. Closing the stream.")
    print(f"Error: {error}")
    return True


def on_stream_closed():
    print("Stream closed.")
    pass


ipc_client = clientV2.GreengrassCoreIPCClientV2()
resp, operation = ipc_client.subscribe_to_iot_core(
    topic_name=subscribe_topic,
    qos=qos,
    on_stream_event=on_stream_event,
    on_stream_error=on_stream_error,
    on_stream_closed=on_stream_closed,
)

# Keep the main thread alive, or the process will exit.
event = threading.Event()
event.wait()

# To stop subscribing, close the operation stream.
operation.close()
ipc_client.close()
