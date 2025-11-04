import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os
import sys
import json

load_dotenv()

# connect to the broker
mqttBroker = os.getenv('MQTT_BROKER')
port = int(os.getenv('PORT'))
topic = os.getenv('TOPIC')
mqtt_client = os.getenv('MQTT_CLIENT')

sleep_time = int(os.getenv('SLEEP_TIME'))

missing = []
if not mqttBroker:
    missing.append('MQTT_BROKER')

# ensure port is valid integer (port may already be int)
try:
    port = int(port)
except Exception:
    missing.append('PORT (missing or not an integer)')

if not topic:
    missing.append('TOPIC')

if not mqtt_client:
    missing.append('MQTT_CLIENT')

try:
    sleep_time = int(sleep_time)
except Exception:
    missing.append('SLEEP_TIME (missing or not an integer)')

if missing:
    print(f"Missing or invalid environment variables: {', '.join(missing)}")
    sys.exit(1)

# Create an MQTT client with a specific client ID, using MQTT 3.1.1 protocol and the latest callback API (version 2)
client = mqtt.Client(client_id=mqtt_client, protocol=mqtt.MQTTv311, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(topic) # subscribe after connecting
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, message):
    payload = message.payload.decode('utf-8', errors='replace')
    try:
        data = json.loads(payload)
        print(f"JSON message received on topic {message.topic}:\n{json.dumps(data, indent=2, ensure_ascii=False)}")
    except json.JSONDecodeError:
        print(f"Non-JSON message received on topic {message.topic}: {payload}")
        return

client.on_connect = on_connect
client.on_message = on_message


try:
    client.connect(mqttBroker, port)
    client.loop_forever()

except KeyboardInterrupt:
    print("Interrupted by user. Shutting down subscriber...")
    client.loop_stop()
    client.disconnect()
