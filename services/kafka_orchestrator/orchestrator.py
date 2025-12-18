from confluent_kafka import Producer, Consumer, KafkaError
from db_utils_orch import *
from redis import Redis
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo
import os, signal, sys, json

load_dotenv()

CET = ZoneInfo("Europe/Rome")
def log(prefix, message):
    """
    Simple logger function.
    """
    print(f"[{datetime.now(CET).strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

class Orchestrator:
    """
    Kafka Orchestrator for handling presence updates and intelligent functionalities.
    """

    def __init__(self):
        """Initialize orchestrator configuration from environment variables."""
        self.running = True
        self.consumer = None
        self.redis_client = None
        self.COMMIT_BATCH_SIZE = 50
        
        # Load environment variables
        self._load_env_config()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _load_env_config(self):
        """Load and validate environment variables."""
        self.redis_host = os.getenv('REDIS_HOST')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.redis_password = os.getenv('REDIS_PASSWORD')

        self.kafka_broker = os.getenv('KAFKA_BROKER')
        self.kafka_topics_raw = os.getenv('KAFKA_TOPICS')
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID')
        self.kafka_cons_client_id = os.getenv('KAFKA_CONSUMER_CLIENT_ID')
        self.kafka_auto_commit = os.getenv('KAFKA_AUTO_COMMIT', 'False').lower() in ('true', '1', 't')

        self.kafka_topic_enriched = os.getenv('KAFKA_ENRICHED_TOPIC')
        self.topic_trigger = os.getenv('KAFKA_TRIGGER_TOPIC')
        self.kafka_prod_client_id = os.getenv('KAFKA_EN_CLIENT_ID')
        self.kafka_acks = os.getenv('KAFKA_ACKS', 'all')
        self.kafka_retries = int(os.getenv('KAFKA_RETRIES', 5))

        self.topic_enriched_raw = os.getenv('KAFKA_ENRICHED_TOPIC')
        self.rooms_list_raw = os.getenv('ROOMS_LIST')
        self.users_email_list_raw = os.getenv('USER_EMAILS_LIST')

        # Validate environment variables
        missing = []
        for var_name, var_value in {
            "KAFKA_BROKER": self.kafka_broker,
            "KAFKA_TOPICS": self.kafka_topics_raw,
            "KAFKA_GROUP_ID": self.kafka_group_id,
            "KAFKA_CLIENT_ID": self.kafka_client_id,
            "REDIS_HOST": self.redis_host,
            "REDIS_PASSWORD": self.redis_password,
            "KAFKA_ENRICHED_TOPIC": self.topic_enriched_raw,
            "ROOMS_LIST": self.rooms_list_raw,
            "USER_EMAILS_LIST": self.users_email_list_raw
        }.items():
            if not var_value:
                missing.append(var_name)

        if missing:
            message = f"Environment variable(s) {', '.join(missing)} are missing or invalid."
            log("SYSTEM - ORCHESTRATOR", message)
            sys.exit(1)

        # Parse lists from environment variables
        try:
            self.kafka_topics = [topic.strip() for topic in self.kafka_topics_raw.split(',')]
            self.rooms_list = [room.strip() for room in self.rooms_list_raw.split(',')]
            self.users_email_list = [email.strip() for email in self.users_email_list_raw.split(',')]
            self.topic_enriched = [topic.strip() for topic in self.topic_enriched_raw.split(',')] if self.topic_enriched_raw else []

        except Exception as e:
            message = f"Error parsing list environment variables: {str(e)}"
            log("SYSTEM - ORCHESTRATOR", message)
            sys.exit(1)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals to gracefully stop the consumer."""
        log("SYSTEM", f"Shutdown signal {signum} received. Stopping...")
        self.running = False

    def _redis_setup(self):
        """Setup Redis client."""
        redis_host = self.redis_host
        redis_port = self.redis_port
        redis_password = self.redis_password
        
        self.redis_client = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True
        )

    def _setup_kafka_consumer(self):
        """Setup Kafka consumer."""
        consumer_config = {
            'bootstrap.servers': self.kafka_broker,
            'group.id': self.kafka_group_id,
            'client.id': self.kafka_client_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': self.kafka_auto_commit,
            'session.timeout.ms': 30000
        }

        try:
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe(self.kafka_topics)
            log("KAFKA_ORCHESTRATOR", f"Subscribed to topics {self.kafka_topics} on broker '{self.kafka_broker}'")
        except Exception as e:
            log("KAFKA_ORCHESTRATOR", f"Failed to setup Kafka consumer: {e}")
            sys.exit(1)

    def _setup_kafka_producer(self):
        """Setup Kafka producer for enriched data."""
        producer_config = {
            'bootstrap.servers': self.kafka_broker,
            'client.id': self.kafka_prod_client_id,
            'acks': self.kafka_acks,
            'retries': self.kafka_retries
        }

        try:
            self.producer = Producer(producer_config)
            log("KAFKA_ORCHESTRATOR", f"Kafka producer setup complete for broker '{self.kafka_broker}'")
        except Exception as e:
            log("KAFKA_ORCHESTRATOR", f"Failed to setup Kafka producer: {e}")
            sys.exit(1)

    def _delivery_report(self, err, msg):
        """
        Callback for message delivery reports.
        """
        if err is not None:
            log("USER_PRESENCE", f"Message delivery failed: {err}")
        else:
            log("USER_PRESENCE", f"Message successfully delivered!")
            log("USER_PRESENCE", f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")

    def _handle_presence_update(self, msg):
        """
        Handle presence update messages.

        Args:
            msg: Kafka message containing presence update.
                """
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log("KAFKA_CONSUMER", f"End of partition reached {msg.topic()} [{msg.partition()}]")
            else:
                log("KAFKA_CONSUMER", f"Error: {msg.error()}")
            return
        try:
            data = json.loads(msg.value().decode('utf-8'))
            room = data.get("room")
            user_email = data.get("user_email", [])
            status = data.get("status")
            
            if room and user_email and status in ["ENTER", "EXIT"]:
                garmin_id = get_garmin_id_by_email(user_email)
                
                if status == "EXIT" and garmin_id:
                    # remove garmin id associated to the room which is the key of the set
                    self.redis_client.srem(f"room_presence:{room}", garmin_id)
                    self.redis_client.delete(f"garmin_room:{garmin_id}")
                    

                elif status == "ENTER" and garmin_id:
                    # add garmin id associated to the room which is the key of the set
                    self.redis_client.sadd(f"room_presence:{room}", garmin_id)
                    self.redis_client.set(f"garmin_room:{garmin_id}", room)

                log("KAFKA_ORCHESTRATOR", f"Updated room '{room}' with users: '{self.redis_client.get(room)}")
            else:
                log("KAFKA_ORCHESTRATOR", f"Invalid presence update message: {data}")
        except Exception as e:
            log("KAFKA_ORCHESTRATOR", f"Failed to process presence update message: {e}")

    def _enrich_data_wearable(self, msg):
        """
        Enrich message data with additional information.

        Args:
            msg: Kafka message object
        Returns:
            Enriched data dictionary
        """
        garmin_id = msg.get('garmin_id')

        if not garmin_id:
            return None
        
        # caso wearable data
        room = self.redis_client.get(f"garmin_room:{garmin_id}")    
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
            data['user_room'] = room
            return data
        except Exception as e:
            log("KAFKA_ORCHESTRATOR", f"Failed to enrich message data: {e}")
            return None
        
    def _enrich_data_ambient(self, msg):
        """
        Enrich ambient message data with additional information.

        Args:
            msg: Kafka message object
        Returns:
            Enriched data dictionary
        """
        room = msg.get('room')

        if not room:
            return None
        try:
            data = json.loads(msg.value().decode('utf-8'))
            users_in_room = [user_id for user_id in self.redis_client.smembers(f"room_presence:{room}")]
            data['users_in_room'] = users_in_room

            log("KAFKA_ORCHESTRATOR", f"Enriched ambient data for room '{room}' with users: {users_in_room}")
            return data
        except Exception as e:
            log("KAFKA_ORCHESTRATOR", f"Failed to enrich ambient message data: {e}")
            return None
        
    def _send_enriched_data(self, enriched_data):



        # DA SISTEMAREEEEEEEEEEEEEEE
        """
        Send enriched data to Kafka topic.

        Args:
            enriched_data: The enriched data dictionary to send.
        """
        try:
            value_bytes = json.dumps(enriched_data, ensure_ascii=False).encode('utf-8')
            key_bytes = str(enriched_data.get('room_id', 'unknown')).encode('utf-8')
            
            self.producer.produce(
                self.kafka_topic_enriched,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            log("KAFKA_ORCHESTRATOR", f"Failed to send enriched data: {e}")
        
    def _enrich_and_trigger(self, msg):
        """
        Send enriched message to the appropriate Kafka topic.

        Args:
            enriched_data: The enriched data dictionary to send.
        """
        topic = msg.topic()

        try:
            data = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError as e:
            log("KAFKA_ORCHESTRATOR", f"Failed to parse message as JSON: {e}")
            return
        
        enriched_data = None
        error = None
        if topic == "wearable_data":
            enriched_data, error = self._enrich_data_wearable(msg)
        elif topic == "sensors_data":
            enriched_data, error = self._enrich_data_ambient(msg)

        if error:
            log("KAFKA_ORCHESTRATOR", f"Error enriching data: {error}")
            return
        
        if enriched_data:
            # Qui andrebbe il Windowing (Logica 2.5) e il Motore di Decisione (Logica 2.6)
            
            # Per ora, inviamo semplicemente il dato arricchito
            self._send_enriched_data(enriched_data)

            topic = self.topic_enriched
            # Placeholder for sending enriched data to Kafka producer
            log("KAFKA_ORCHESTRATOR", f"Sending enriched data: {enriched_data}")
            # Here you would implement the Kafka producer logic to send the enriched data
    def run(self):
        """Main consumer loop - polls Kafka and processes messages."""
        messages_count = 0
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
                
                if msg.topic() == "presence_updates":
                    self._handle_presence_update(msg)
                else:
                    # we wil manage the intelligent functionality here
                    log("KAFKA_ORCHESTRATOR", f"Received message on unknown topic '{msg.topic()}'")

                if not self.kafka_auto_commit:
                    messages_count += 1    

                if messages_count >= self.COMMIT_BATCH_SIZE:
                    self.consumer.commit(asynchronous=False)
                    log("KAFKA_ORCHESTRATOR", f"Committed batch of {messages_count} messages.")
                    messages_count = 0

            # Final commit on shutdown
            if not self.kafka_auto_commit and messages_count > 0:
                self.consumer.commit(asynchronous=False)
                log("KAFKA_ORCHESTRATOR", f"Final commit of {messages_count} messages on shutdown.")
            
        except Exception as e:
            log("KAFKA_CONSUMER", f"Exception occurred: {str(e)}")
        finally:
            self.close()

    def setup(self):
        """Setup orchestrator components."""
        self._redis_setup()
        self._setup_kafka_consumer()
        # self.setup_kafka_producer() FOR LATER