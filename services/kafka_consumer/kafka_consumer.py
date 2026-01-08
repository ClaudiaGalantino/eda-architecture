from confluent_kafka import Consumer, KafkaError
from collections import defaultdict
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
from zoneinfo import ZoneInfo
import json, os, sys, signal

load_dotenv()

CET = ZoneInfo("Europe/Rome")
def log(prefix, message):
    """
    Simple logger function.
    """
    print(f"[{datetime.now(CET).strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

class KafkaMongoConsumer:
    """
    Consumes messages from Kafka topics and writes them to MongoDB collections.
    Supports topic-to-collection mapping.
    """
    
    def __init__(self):
        """Initialize consumer configuration from environment variables."""
        self.running = True
        self.mongo_client = None
        self.mongo_db = None
        self.consumer = None
        self.topic_collection_map_dict = {}
        self.message_counter = 0
        self.message_batch = defaultdict(list)
        self.allowed_garmin_ids = []
        
        # Load environment variables
        self._load_env_config()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
    
    def _load_env_config(self):
        """Load and validate environment variables."""
        self.mongo_uri = os.getenv('MONGO_URI')
        self.mongo_db_name = os.getenv('MONGO_DB')
        self.mongo_batch_size = int(os.getenv('MONGO_BATCH_SIZE', 5))
        self.topic_collection_map = os.getenv('TOPIC_COLLECTION_MAP')
        
        self.kafka_broker = os.getenv('KAFKA_BROKER')
        self.kafka_topics_raw = os.getenv('KAFKA_TOPICS')
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID')
        self.kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
        self.kafka_auto_commit = os.getenv('KAFKA_AUTO_COMMIT', 'False').lower() in ('true', '1', 't')
        self.allowed_garmin_ids_raw = os.getenv('ALLOWED_GARMIN_IDS')
        
        # Validate environment variables
        missing = []
        for var_name, var_value in {
            "KAFKA_BROKER": self.kafka_broker,
            "KAFKA_TOPICS": self.kafka_topics_raw,
            "KAFKA_GROUP_ID": self.kafka_group_id,
            "KAFKA_CLIENT_ID": self.kafka_client_id,
            "MONGO_URI": self.mongo_uri,
            "MONGO_DB": self.mongo_db_name,
            "TOPIC_COLLECTION_MAP": self.topic_collection_map,
            "ALLOWED_GARMIN_IDS": self.allowed_garmin_ids_raw
        }.items():
            if not var_value:
                missing.append(var_name)
        
        if missing:
            message = f"Environment variable(s) {', '.join(missing)} are missing or invalid."
            log("SYSTEM - KAFKA_CONSUMER", message)
            sys.exit(1)
        
        # Parse topics
        self.kafka_topics = [t.strip() for t in self.kafka_topics_raw.split(',') if t.strip()]
        
        if not self.kafka_topics:
            log("SYSTEM - KAFKA_CONSUMER", "No Kafka topics found in KAFKA_TOPICS. Provide at least one topic (comma-separated if many).")
            sys.exit(1)

        # Parse allowed Garmin IDs
        if self.allowed_garmin_ids_raw:
            self.allowed_garmin_ids = [gid.strip() for gid in self.allowed_garmin_ids_raw.split(',') if gid.strip()]
            log("SYSTEM - KAFKA_CONSUMER", f"Filtering wearable data for Garmin IDs: {self.allowed_garmin_ids}")
        else:
            log("SYSTEM - KAFKA_CONSUMER", "No ALLOWED_GARMIN_IDS configured, all wearable data will be saved.")
    
    def _parse_topic_collection_mapping(self):
        """Parse topic to collection mapping from environment variable."""
        for pair in self.topic_collection_map.split(','):
            if ':' in pair:
                topic, collection = pair.split(':', 1)
                topic = topic.strip()
                collection = collection.strip()
                if topic and collection:
                    self.topic_collection_map_dict[topic] = collection
    
    def _setup_mongodb(self):
        """Connect to MongoDB and initialize database."""
        try:
            self.mongo_client = MongoClient(self.mongo_uri)
            self.mongo_db = self.mongo_client[self.mongo_db_name]
            log("MONGO", f"Connected to MongoDB at {self.mongo_uri}")
        except Exception as e:
            log("MONGO", f"Failed to connect to MongoDB: {e}")
            sys.exit(1)
    
    def _setup_kafka_consumer(self):
        """Setup Kafka consumer configuration and subscription."""
        consumer_conf = {
            'bootstrap.servers': self.kafka_broker,
            'group.id': self.kafka_group_id,
            'client.id': self.kafka_client_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': self.kafka_auto_commit,
            'session.timeout.ms': 30000
        }
        
        try:
            self.consumer = Consumer(consumer_conf)
            self.consumer.subscribe(self.kafka_topics)
            log("KAFKA_CONSUMER", f"Subscribed to topics {self.kafka_topics} on broker '{self.kafka_broker}'")
        except Exception as e:
            log("KAFKA_CONSUMER", f"Failed to setup Kafka consumer: {e}")
            sys.exit(1)
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals to gracefully stop the consumer."""
        log("SYSTEM", f"Shutdown signal {signum} received. Stopping...")
        self.running = False

    def _write_batch_to_mongodb(self):
        """Write accumulated messages to MongoDB in batch."""
        if not self.message_batch:
            return
        log("MONGO", f"Flushing batch of {sum(len(v) for v in self.message_batch.values())} documents...")

        try:
            # Insert documents in batch per collection
            for collection_name, docs in self.message_batch.items():
                if docs:
                    collection = self.mongo_db[collection_name]
                    result = collection.insert_many(docs)
                    log("MONGO", f"Inserted {len(result.inserted_ids)} documents into {collection_name}")
            
            # Offests commit 
            if not self.kafka_auto_commit:
                self.consumer.commit(asynchronous=False)
                log("KAFKA_CONSUMER", "Offsets committed after batch insert.")
            # Clear batch data
            self.message_batch.clear()
            self.message_counter = 0
        except Exception as e:
            log("MONGO", f"Failed to insert batch documents: {e}")
    
    def _process_message(self, msg):
        """
        Process a single Kafka message.
        
        Args:
            msg: Kafka message object
        """
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log("KAFKA_CONSUMER", f"End of partition reached {msg.topic()} [{msg.partition()}]")
            else:
                log("KAFKA_CONSUMER", f"Error: {msg.error()}")
            return
        
        topic = msg.topic()
        log("KAFKA_CONSUMER", f"Received message! Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")
        
        try:
            doc = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError as e:
            log("KAFKA_CONSUMER", f"Failed to parse message as JSON: {e}")
            return
        
        # Filter wearable data by allowed Garmin IDs
        if topic == 'wearable_data' and self.allowed_garmin_ids:
            garmin_id = doc.get('garmin_id')
            if not garmin_id:
                log("KAFKA_CONSUMER", f"Skipping wearable message without garmin_id")
                return
            if garmin_id not in self.allowed_garmin_ids:
                log("KAFKA_CONSUMER", f"Skipping wearable message for non-allowed garmin_id: {garmin_id}")
                return
            log("KAFKA_CONSUMER", f"Accepted wearable data for garmin_id: {garmin_id}")
        
        # Get collection name from mapping
        collection_name = self.topic_collection_map_dict.get(topic)
        if not collection_name:
            log("KAFKA_CONSUMER", f"No collection mapping found for topic '{topic}'")
            return
        
        # Insert document into Batch
        self.message_batch[collection_name].append(doc)
        self.message_counter += 1

        if self.message_counter >= self.mongo_batch_size:
            self._write_batch_to_mongodb()
    
    def setup(self):
        """Initialize all connections and configurations."""
        self._parse_topic_collection_mapping()
        self._setup_mongodb()
        self._setup_kafka_consumer()
    
    def run(self):
        """Main consumer loop - polls Kafka and processes messages."""
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        log("KAFKA_CONSUMER", f"Error: {msg.error()}")
                        continue
                
                self._process_message(msg)

            # Flush any remaining messages in batch before shutdown
            self._write_batch_to_mongodb()

        except Exception as e:
            log("KAFKA_CONSUMER", f"Exception occurred: {str(e)}")
        finally:
            self.close()
    
    def close(self):
        """Close all connections and cleanup resources."""
        if self.consumer:
            self.consumer.close()
            log("KAFKA_CONSUMER", "Consumer closed.")
        if self.mongo_client:
            self.mongo_client.close()
            log("MONGO", "MongoDB connection closed.")
        log("KAFKA_CONSUMER", "Exiting.")


if __name__ == "__main__":
    consumer = KafkaMongoConsumer()
    consumer.setup()
    consumer.run()