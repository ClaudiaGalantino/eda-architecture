import json
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os
import sys
import signal

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

# Mongo DB setup
mongo_uri = os.getenv('MONGO_URI')
mongo_db_name = os.getenv('MONGO_DB')
topic_collection_map = os.getenv('TOPIC_COLLECTION_MAP')


# Kafka setup
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topics_raw = os.getenv('KAFKA_TOPICS')
kafka_group_id = os.getenv('KAFKA_GROUP_ID')
kafka_client_id = os.getenv('KAFKA_CLIENT_ID')


# Validate environment variables
missing = []
for var_name, var_value in {
    "KAFKA_BROKER": kafka_broker,
    "KAFKA_TOPICS": kafka_topics_raw,
    "KAFKA_GROUP_ID": kafka_group_id,
    "KAFKA_CLIENT_ID": kafka_client_id,
    "MONGO_URI": mongo_uri,
    "MONGO_DB": mongo_db_name,
    "TOPIC_COLLECTION_MAP": topic_collection_map
}.items():
    if not var_value:
        missing.append(var_name)

if missing:
    print(f"Missing or invalid environment variables: {', '.join(missing)}")
    sys.exit(1)

# Parse topics
kafka_topics = [t.strip() for t in kafka_topics_raw.split(',') if t.strip()]

if not kafka_topics:
    print("No Kafka topics found in KAFKA_TOPICS. Provide at least one topic (comma-separated if many).")
    sys.exit(1)

# MongoDB client
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db_name]

# Parse topic to collection mapping
topic_collection_map_dict = {}

for pair in topic_collection_map.split(','):
    if ':' in pair:
        topic, collection = pair.split(':', 1)
        if topic and collection:
            topic_collection_map_dict[topic] = collection

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': kafka_broker,
    'group.id': kafka_group_id,
    'client.id': kafka_client_id,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(kafka_topics)
log("KAFKA_CONSUMER", f"Subscribed to topics {kafka_topics} on broker '{kafka_broker}'")
try:
    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log("KAFKA_CONSUMER", f"End of partition reached {msg.topic()} [{msg.partition()}]")
            else:
                log("KAFKA_CONSUMER", f"Error: {msg.error()}")
        else:
            topic = msg.topic()
            log("KAFKA_CONSUMER", f"Received message! \n Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")
            doc = json.loads(msg.value().decode('utf-8'))
            
            # write data to MongoDB
            collection_name = topic_collection_map_dict.get(topic)
            if collection_name:
                collection = mongo_db[collection_name]
                try:
                    collection.insert_one(doc)
                    log("MONGO", f"Inserted document into {collection_name}")            
                except Exception as e:
                    log("MONGO", f"Failed to insert document {json.dumps(doc, indent=2, ensure_ascii=False, default=str)} into {collection_name}.\n Error: {e}")

except Exception as e:
    log("KAFKA_CONSUMER", f"Exception occurred: {str(e)}")
finally:
    consumer.close()
    log("KAFKA_CONSUMER", "Consumer closed. Exiting.")