from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime
import os
import sys
import json

load_dotenv()

def log(prefix, message):
    """
    Simple logger function.
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

# Kafka setup
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = os.getenv('KAFKA_TOPIC')
kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
kafka_acks = os.getenv('KAFKA_ACKS', 'all')
kafka_retries = int(os.getenv('KAFKA_RETRIES', 5))
url = os.getenv('URL')
files = os.getenv('FILE_PATH')

# Validate environment variables
missing = []
for var_name, var_value in {
    "KAFKA_BROKER": kafka_broker,
    "KAFKA_TOPIC": kafka_topic,
    "KAFKA_CLIENT_ID": kafka_client_id,
    "URL": url,
    "FILE_PATH": files,
}.items():
    if not var_value:
        missing.append(var_name)

if missing:
    log("SYSTEM", f"Missing or invalid environment variables: {', '.join(missing)}")
    sys.exit(1)

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': kafka_broker,
    'client.id': kafka_client_id,
    'acks': kafka_acks,
    'retries': kafka_retries,
}

producer = Producer(producer_conf)

def start_producer():
    """
    Start the Kafka producer if not already started.
    """
    global producer
    if producer is None:
        producer = Producer(producer_conf)
        log("KAFKA", "Producer started.")

def delivery_report(err, msg):
    """
    Callback for message delivery reports.
    """
    if err is not None:
        log("KAFKA", f"Message delivery failed: {err}")
    else:
        log("KAFKA", f"Message successfully delivered!")
        log("KAFKA", f"Content: {json.dumps(json.loads(msg.value().decode('utf-8')), indent=2, ensure_ascii=False)}")
        log("KAFKA", f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")

def close_producer():
    """
    Close the Kafka producer.
    """
    global producer
    if producer is not None:
        producer.flush(timeout=10)
        log("KAFKA", "Producer flushed and closed.")
        producer = None

def send_data(record):
    """
    Send data to Kafka topic.

    Args:
        record (dict): The data record to send.
    Returns:
        Sends the record to the configured Kafka topic.
    """
    if producer is None:
        start_producer()
    try:
        message = json.dumps(record).encode('utf-8')
        producer.produce(
            kafka_topic,
            value=message,
            callback=delivery_report
        )
        producer.poll(0)
        log("KAFKA", f"Produced data!")
    except Exception as e:
        log("KAFKA", f"Exception occurred: {e}")
