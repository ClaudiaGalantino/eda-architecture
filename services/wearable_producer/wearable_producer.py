from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime
import os
import sys
import signal
import json
import time

load_dotenv()

def log(prefix, message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

running = True
def handle_shutdown(signum, frame):
    global running
    log("SYSTEM", f"Shutdown signal {signum} received. Stopping...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def get_reference_data():
    for filename in os.listdir("processed_data"):
        if filename.endswith (".json"):
            input_path = os.path.join("processed_data", filename)
            with open(input_path, 'r') as f:
                data = json.load(f)
                for record in data:
                    ref_date = record["date"]
                    return ref_date
                
def get_data_to_produce():
    ref_date = get_reference_data()

    data_list = []
    
    for filename in os.listdir("processed_data"):
        if filename.endswith (".json"):
            input_path = os.path.join("processed_data", filename)
            with open(input_path, 'r') as f:
                data = json.load(f)
                for record in data:
                    if record["date"] == ref_date:
                        data_list.append(record)
    return data_list             

# Kafka setup
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = os.getenv('KAFKA_TOPIC')
kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
kafka_acks = os.getenv('KAFKA_ACKS', 'all')
kafka_retries = int(os.getenv('KAFKA_RETRIES', 5))

# Validate environment variables
missing = []
for var_name, var_value in {
    "KAFKA_BROKER": kafka_broker,
    "KAFKA_TOPIC": kafka_topic,
    "KAFKA_CLIENT_ID": kafka_client_id,
}.items():
    if not var_value:
        missing.append(var_name)

if missing:
    print(f"Missing or invalid environment variables: {', '.join(missing)}")
    sys.exit(1)

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': kafka_broker,
    'client.id': kafka_client_id,
    'acks': kafka_acks,
    'retries': kafka_retries,
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        log("KAFKA", f"Message delivery failed: {err}")
    else:
        log("KAFKA", f"Message successfully delivered!")
        log("KAFKA", f"Content: {json.dumps(json.loads(msg.value().decode('utf-8')), indent=2, ensure_ascii=False)}")
        log("KAFKA", f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")

def retrieve_wearable_data():
    log("KAFKA", f"Starting to produce data to topic '{kafka_topic}' on broker '{kafka_broker}'")
try:
    # Use test data
    data_list = get_data_to_produce()
    if len(data_list) <= 1:
        if len(data_list) == 1:
            log("KAFKA", "1 record found to produce. Retrying to find more data.")
            data_list = get_data_to_produce()
        else:
            log("KAFKA", "No records found to produce. Exiting.")

    for record in data_list:
        message = json.dumps(record).encode('utf-8')
        producer.produce(
            kafka_topic,
            value=message, 
            callback=delivery_report
        )
        producer.poll(0)
        log("KAFKA", f"Produced data for user {record.get('user_id')}")
    while running:
         time.sleep(5)
except Exception as e:
    log("KAFKA", f"Exception occurred: {e}")
finally:
    producer.flush(timeout=10)
    log("KAFKA", "Producer flushed and shutting down.")

