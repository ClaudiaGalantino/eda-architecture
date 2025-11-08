import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime
import os
import sys
import json
import signal
import time

load_dotenv()

def log (prefix, message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

running = True
def handle_shutdown(signum, frame):
    global running
    log("SYSTEM", f"Shutdown signal {signum} received. Stopping...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# MQTT setup
mqtt_broker = os.getenv('MQTT_BROKER')
mqtt_port = int(os.getenv('MQTT_PORT'))
mqtt_topic = os.getenv('MQTT_TOPIC')
mqtt_client = os.getenv('MQTT_CLIENT')

# Kafka setup
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = os.getenv('KAFKA_TOPIC')
kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
kafka_acks = os.getenv('KAFKA_ACKS', 'all')
kafka_retries = int(os.getenv('KAFKA_RETRIES', 5))

sleep_time = int(os.getenv('SLEEP_TIME'), 30)

# Validate environment variables
missing = []
for var_name, var_value in {
    "MQTT_BROKER": mqtt_broker,
    "MQTT_PORT": mqtt_port,
    "MQTT_TOPIC": mqtt_topic,
    "MQTT_CLIENT": mqtt_client,
    "KAFKA_BROKER": kafka_broker,
    "KAFKA_TOPIC": kafka_topic,
    "KAFKA_CLIENT_ID": kafka_client_id,
}.items():
    if not var_value:
        missing.append(var_name)

if missing:
    print(f"Missing or invalid environment variables: {', '.join(missing)}")
    sys.exit(1)

# MQTT client
client = mqtt.Client(
    client_id=mqtt_client,
    protocol=mqtt.MQTTv311, 
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)

# Kafka producer
kafka_conf = {
    'bootstrap.servers': kafka_broker,
    'client.id': kafka_client_id,
    'acks': kafka_acks,
    'retries': kafka_retries
}
producer = Producer(kafka_conf)

def delivery_report(err, msg):
    if err is not None:
        log("KAFKA", f"Message delivery failed: {err}")
    else:
        log("KAFKA", f"Message successfully delivered!")
        log("KAFKA", f"Content: {json.dumps(json.loads(msg.value().decode('utf-8')), indent=2, ensure_ascii=False)}")
        log("KAFKA", f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log("MQTT", "Connected to MQTT Broker!")
        client.subscribe(mqtt_topic)
    else:
        log("MQTT", f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):

    payload = msg.payload.decode('utf-8', errors='replace')
    try:

        data_received = json.loads(payload)
        log("MQTT", f"JSON message received on topic {msg.topic}:\n{json.dumps(data_received, indent=2, ensure_ascii=False)}")
        
        # convert to bytes, handle missing key
        key = data_received.get("room_name")
        key_bytes = key.encode('utf-8') if key is not None else None
        value_bytes = json.dumps(data_received, ensure_ascii=False).encode('utf-8')
        
        producer.produce(
            kafka_topic, 
            key=key_bytes,
            value=value_bytes,
            callback=delivery_report
        )
    except json.JSONDecodeError:
        log("MQTT", f"Non-JSON message received on topic {msg.topic}: {payload}")
        return
    
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(mqtt_broker, mqtt_port)
    client.loop_start()
    while running:
        producer.poll(1)
        time.sleep(sleep_time)

except Exception as e:
    log("SYSTEM", f"Error occurred: {e}")
    running = False
    
finally:
    client.loop_stop()
    client.disconnect()
    producer.flush(timeout=10)
    log("SYSTEM", "MQTT-Kafka bridge stopped.")