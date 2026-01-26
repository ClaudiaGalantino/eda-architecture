from confluent_kafka import Producer, Consumer, KafkaError
from datetime import datetime, timezone, timedelta
from db_utils_orch import *
from redis import Redis
from pymongo import MongoClient
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import os, signal, sys, json
import pandas as pd

load_dotenv()

CET = ZoneInfo("Europe/Rome")
def log(prefix, message):
    """
    Simple logger function.
    """
    print(f"[{datetime.now(CET).strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

# ------------------------------------------------------ ORCHESTRATOR CLASS --------------------------------------------------------

class Orchestrator:
    """
    Kafka Orchestrator for handling presence updates and intelligent functionalities.
    """

    def __init__(self):
        """Initialize orchestrator configuration from environment variables."""       
        self.running = True
        self.consumer = None
        self.redis_client = None
        self.mongo_client = None
        self.mongo_db = None
        self.producer = None
        self.wearable_buffer = []
        self.BATCH_SIZE = 5
        self.WORKING_HOURS= [(9,12), (14,18)] # 9AM-12PM and 2PM-6PM 
        # Absolute output directory mapped by Docker: ./data/orchestrator -> /app/orchestrator_data
        self.output_dir = os.getenv('ORCH_DATA_DIR', '/app/orchestrator_data')

        # Load environment variables
        self._load_env_config()

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

# -------------------------------------------------- LOAD ENV VARS AND VALIDATE -------------------------------------------------

    def _load_env_config(self):
        """Load and validate environment variables."""
        self.redis_host = os.getenv('REDIS_HOST')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.mongo_uri = os.getenv('MONGO_URI')
        self.mongo_db_name = os.getenv('MONGO_DB_NAME')
        self.sensor_en_data_collection = os.getenv('SENSOR_EN_DATA_COLLECTION')
        self.rooms_list_raw = os.getenv('ROOMS_LIST')
        self.users_email_list_raw = os.getenv('USER_EMAILS_LIST')
        self.kafka_broker = os.getenv('KAFKA_BROKER')
        self.kafka_topics_raw = os.getenv('KAFKA_TOPICS')
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID')
        self.kafka_consumer_client_id = os.getenv('KAFKA_CONSUMER_CLIENT_ID')
        self.kafka_auto_commit = os.getenv('KAFKA_AUTO_COMMIT', 'False').lower() in ('true', '1', 't')
        self.kafka_trigger_client_id = os.getenv('KAFKA_TR_CLIENT_ID')
        self.kafka_trigger_topic = os.getenv('KAFKA_TRIGGER_TOPIC')
        self.kafka_prod_client_id = os.getenv('KAFKA_EN_CLIENT_ID')
        self.topic_enriched_raw = os.getenv('KAFKA_ENRICHED_TOPICS')
        self.kafka_acks = os.getenv('KAFKA_ACKS', 'all')
        self.kafka_retries = int(os.getenv('KAFKA_RETRIES', 5))
        self.allowed_garmin_ids_raw = os.getenv('ALLOWED_GARMIN_IDS')

        # Validate environment variables
        missing = []
        for var_name, var_value in {
            "REDIS_HOST": self.redis_host,
            "MONGO_URI": self.mongo_uri,
            "MONGO_DB_NAME": self.mongo_db_name,
            "SENSOR_EN_DATA_COLLECTION": self.sensor_en_data_collection,
            "ROOMS_LIST": self.rooms_list_raw,
            "USER_EMAILS_LIST": self.users_email_list_raw,
            "KAFKA_BROKER": self.kafka_broker,
            "KAFKA_TOPICS": self.kafka_topics_raw,
            "KAFKA_GROUP_ID": self.kafka_group_id,
            "KAFKA_CONSUMER_CLIENT_ID": self.kafka_consumer_client_id,
            "KAFKA_TR_CLIENT_ID": self.kafka_trigger_client_id,
            "KAFKA_TRIGGER_TOPIC": self.kafka_trigger_topic,
            "KAFKA_EN_CLIENT_ID": self.kafka_prod_client_id,
            "KAFKA_ENRICHED_TOPICS": self.topic_enriched_raw,
            "ALLOWED_GARMIN_IDS": self.allowed_garmin_ids_raw,
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
            self.allowed_garmin_ids = [gid.strip() for gid in self.allowed_garmin_ids_raw.split(',') if gid.strip()]

        except Exception as e:
            message = f"Error parsing list environment variables: {str(e)}"
            log("SYSTEM - ORCHESTRATOR", message)
            sys.exit(1)

# ------------------------------------------------------ SIGNAL HANDLERS ----------------------------------------------------------

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals to gracefully stop the consumer."""
        log("SYSTEM", f"Shutdown signal {signum} received. Stopping...")
        self.running = False

# ------------------------------------------------------ SETUP COMPONENTS ---------------------------------------------------------
  
    def _redis_setup(self):
        """Setup Redis client."""
        try:
            self.redis_client = Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True
            )
        except Exception as e:
            log("ORCH_REDIS", f"Failed to connect to Redis: {e}")
            sys.exit(1)

    def _mongo_setup(self):
        """Setup MongoDB client."""
        try:
            self.mongo_client = MongoClient(self.mongo_uri)
            self.mongo_db = self.mongo_client[self.mongo_db_name]
            #log("ORCH_MONGO", f"Connected to MongoDB database '{self.mongo_db_name}'")
        except Exception as e:
            log("ORCH_MONGO", f"Failed to connect to MongoDB: {e}")
            sys.exit(1)

    def _setup_kafka_consumer(self):
        """Setup Kafka consumer."""
        consumer_config = {
            'bootstrap.servers': self.kafka_broker,
            'group.id': self.kafka_group_id,
            'client.id': self.kafka_consumer_client_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': self.kafka_auto_commit,
            'session.timeout.ms': 30000
        }
        try:
            self.consumer = Consumer(consumer_config)
            topics_to_subscribe = list(set(self.kafka_topics + (self.topic_enriched or [])))
            self.consumer.subscribe(topics_to_subscribe)
            log("ORCH_CONSUMER", f"Subscribed to topics {topics_to_subscribe} on broker '{self.kafka_broker}'")
        except Exception as e:
            log("ORCH_CONSUMER", f"Failed to setup Kafka consumer: {e}")
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
            log("ORCH_PRODUCER", f"Kafka producer setup complete for broker '{self.kafka_broker}'")
        except Exception as e:
            log("ORCH_PRODUCER", f"Failed to setup Kafka producer: {e}")
            sys.exit(1)

    def _delivery_report(self, err, msg):
        """
        Callback for message delivery reports.
        """
        if err is not None:
            log("ORCH", f"Message delivery failed: {err}")
        else:
            log("ORCH", f"Message successfully delivered!")
            log("ORCH", f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")

# ------------------------------------------------------ PRESENCE UPDATES --------------------------------------------------------

    def _handle_presence_update(self, msg):
        """
        Handle presence update messages.

        Args:
        msg: Kafka message containing presence update.
            """
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log("ORCH_CONSUMER", f"End of partition reached {msg.topic()} [{msg.partition()}]")
            else:
                log("ORCH_CONSUMER", f"Error: {msg.error()}")
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
                    users_in_room = self.redis_client.smembers(f"room_presence:{room}")
                    log("ORCH_REDIS", f"Updated room '{room}' with users: {users_in_room}")
                elif status == "ENTER" and garmin_id:
                    # add garmin id associated to the room which is the key of the set
                    self.redis_client.sadd(f"room_presence:{room}", garmin_id)
                    self.redis_client.set(f"garmin_room:{garmin_id}", room)
                    users_in_room = self.redis_client.smembers(f"room_presence:{room}")
                    log("ORCH_REDIS", f"Updated room '{room}' with users: {users_in_room}")
            else:
                log("ORCH_REDIS", f"Invalid presence update message: {data}")
        except Exception as e:
            log("ORCH_REDIS", f"Failed to process presence update message: {e}")

# ------------------------------------------------------ DATA ENRICHMENT --------------------------------------------------------

    def _enrich_data_wearable(self, msg):
        """
        Enrich message data with additional information.

        Args:
            msg: Kafka message object
        Returns:
            Tuple of (enriched_data, error) - enriched_data is None if error occurred
        """
        try:
            data = json.loads(msg.value().decode('utf-8'))
            garmin_id = data.get('garmin_id')
            if not garmin_id:
                return None, "Missing garmin_id in message"
            if self.allowed_garmin_ids and garmin_id not in self.allowed_garmin_ids:
                log("ORCH_ENRICH", f"Skipping wearable message for non-allowed garmin_id: {garmin_id}")
                return None, None
            room = self.redis_client.get(f"garmin_room:{garmin_id}")
            if room is not None:
                data['user_room'] = room
                log("ORCH_ENRICH", f"Enriched wearable data for garmin_id '{garmin_id}' with room: {room}")
            else:
                log("ORCH_ENRICH", f"No room found for garmin_id '{garmin_id}' in Redis.")
                log("ORCH_ENRICH", f"Wearable data for garmin_id '{garmin_id}' remains unenriched. It is saved in raw collection only.")
            return data, None
        except Exception as e:
            log("ORCH_ENRICH", f"Failed to enrich wearable message: {e}")
            return None, str(e)
    
    def _enrich_data_ambient(self, msg):
        """
        Enrich ambient message data with additional information.
        Args:
            msg: Kafka message object
        Returns:
            Tuple of (enriched_data, error) - enriched_data is None if error occurred
        """
        try:
            data = json.loads(msg.value().decode('utf-8'))
            room = data.get('room_name')
            if not room:
                return None, "Missing room_name in message"        
            users_in_room = [user_id for user_id in self.redis_client.smembers(f"room_presence:{room}")]
            data['users_in_room'] = users_in_room
            # Add to mongo collection
            self.mongo_db[self.sensor_en_data_collection].insert_one(data)
            if '_id' in data:
                del data['_id']
            log("ORCH_ENRICH", f"Enriched ambient data for room '{room}' with users: {users_in_room}")
            return data, None      
        except Exception as e:
            log("ORCH_ENRICH", f"Failed to enrich ambient message: {e}")
            return None, str(e)
        
    def _get_enriched_topic_for(self, source_topic):
        """
        Select the appropriate enriched topic string based on the source topic.
        Falls back gracefully if explicit names aren't found.
        """
        candidates = self.topic_enriched or []
        if not candidates:
            raise ValueError("No enriched topics configured")

        if source_topic == "wearable_data":
            for t in candidates:
                if "wearable" in t:
                    return t
            # fallback to first if no wearable-named topic
            return candidates[0]

        if source_topic == "sensors_data":
            for t in candidates:
                if "sensor" in t or "ambient" in t:
                    return t
            # fallback to second if available, else first
            return candidates[1] if len(candidates) > 1 else candidates[0]
        # unknown source topic: fallback to first
        return candidates[0]

    def _send_enriched_data(self, enriched_data, target_topic):
        """
        Send enriched data to Kafka topic.
        Args:
            enriched_data: The enriched data dictionary to send.
        """
        try:
            room_key = (enriched_data.get('room_name') or 
                enriched_data.get('user_room') or 
                'unknown')      
            value_bytes = json.dumps(enriched_data, ensure_ascii=False).encode('utf-8')
            key_bytes = str(room_key).encode('utf-8')
            self.producer.produce(
                target_topic,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            log("ORCH_ENRICH", f"Failed to send enriched data: {e}")

    def _enrich_and_trigger(self, msg):
        """
        Enrich message and send to the appropriate Kafka topic.
        Args:
            msg: Kafka message object to process
        """
        topic = msg.topic()
        enriched_data = None
        error = None
        if topic == "wearable_data":
            enriched_data, error = self._enrich_data_wearable(msg)
        elif topic == "sensors_data":
            enriched_data, error = self._enrich_data_ambient(msg)
        else:
            log("ORCH_ENRICH", f"Unknown topic: {topic}")
            return
        if error:
            log("ORCH_ENRICH", f"Error enriching data: {error}")
            return
        if enriched_data:
            try:
                target_topic = self._get_enriched_topic_for(topic)
                if target_topic == 'wearable_enriched_data':
                    self._send_enriched_data(enriched_data, target_topic)
            except Exception as e:
                log("ORCH_ENRICH", f"Failed selecting enriched topic: {e}")
      
# -------------------------------------------------- WEARABLE DATA PROCESSING --------------------------------------------------

    def _is_working_hour(self, datetime):
        """
        Check if the given datetime is within working hours.
        Args:
            datetime: datetime object to check
        Returns:
            bool: True if within working hours, False otherwise
        """
        hour = datetime.hour
        for start, end in self.WORKING_HOURS:
            if start <= hour <= end:
                return True
        return False
    
    def _sync_with_ambient_data(self, df_wearable, user_room):
        """
        Synchronize wearable data with ambient data from MongoDB.
        Args:
            df_wearable: DataFrame containing wearable data
            user_room: Room associated with the user
        """
        try:
            if df_wearable.empty:
                log("ORCH_SYNC", "No wearable data provided for synchronization.")
                return
            df_wearable['timestamp_local'] = pd.to_datetime(df_wearable['timestamp_local']).dt.tz_localize(None)
            df_wearable.sort_values('timestamp_local')
            # Handle missing user_room by saving wearable-only CSV and skipping ambient query
            if not user_room:
                log("ORCH_SYNC", "Missing user_room; saving wearable-only CSV.")
                df_final = df_wearable.copy()
                os.makedirs(self.output_dir, exist_ok=True)
                csv_path = os.path.join(self.output_dir, f"df_unknown_room_{datetime.now().strftime('%H%M%S')}.csv")
                df_final.to_csv(csv_path, index=False)
                log("ORCH_SYNC", f"CSV scritto senza ambient (room unknown): {csv_path}")
                try:
                    self.mongo_db["merged_df_collection"].insert_many(df_final.to_dict('records'))
                except Exception as e:
                    log("ORCH_SYNC", f"Mongo insert failed (room unknown): {e}")
                return
            # Determine time range for ambient data query
            start_ts = df_wearable['timestamp_local'].min().strftime("%Y-%m-%d %H:%M:%S")
            end_ts = df_wearable['timestamp_local'].max().strftime("%Y-%m-%d %H:%M:%S")
            query = {
                "room_name": user_room,
                "timestamp": {
                    "$gte": start_ts,
                    "$lte": end_ts
                }
            }
            ambient_cursor = self.mongo_db[self.sensor_en_data_collection].find(query)
            df_ambient_raw = pd.DataFrame(list(ambient_cursor))

            if df_ambient_raw.empty:
                log("ORCH_SYNC", f"No ambient data found for room '{user_room}' between {start_ts} and {end_ts}. Saving wearable-only CSV.")
                df_final = df_wearable
            else:
                # Convert ambient timestamp to timezone-aware UTC to match wearable data
                df_ambient_raw['timestamp'] = pd.to_datetime(df_ambient_raw['timestamp']).dt.tz_localize(None)
                df_ambient_raw = df_ambient_raw.sort_values('timestamp')
                # Drop useless columns 
                df_ambient_raw = df_ambient_raw.drop(columns=['_id', 'room_name', 'mqtt_topic', 'users_in_room'], errors='ignore')

                # ASYNC MERGE STRATEGY
                # Merge wearable and ambient data on timestamp, garmin_id, and room
                df_final = pd.merge_asof(
                    df_wearable,
                    df_ambient_raw,
                    left_on='timestamp_local', 
                    right_on='timestamp', 
                    direction='backward' # Prendi l'ultimo valore prima del timestamp Garmin
                ). drop(columns=['timestamp'], errors='ignore')

                # Forward Fill for missing ambient data
                # If the first Garmin data is before the first sensor data, carry forward the values
                df_final = df_final.ffill().bfill()

            log("ORCH_SYNC", f"Final Multimodal Frame ready: {df_final.shape[0]} rows.")
            # --- TEST ---
            log("ORCH_SYNC", "Checking Fused DataFrame Structure:")
            print("\n--- HEAD DEL DATAFRAME FUSO ---")
            print(df_final.head(10).to_string()) 
            print("\n--- VALORI MANCANTI PER COLONNA ---")
            print(df_final.isnull().sum())
            print("\n--- INFO TIPI DATI ---")
            print(df_final.dtypes)
            # save into mongo db
            try:
                self.mongo_db["merged_df_collection"].insert_many(df_final.to_dict('records'))
                log("ORCH_SYNC", f"Saved {len(df_final)} rows to MongoDB 'merged_df_collection'.")
            except Exception as e:
                log("ORCH_SYNC", f"Mongo insert failed (continuing to CSV): {e}")
            # save to CSV for manual inspection (absolute path inside container)
            garmin_id = df_wearable['garmin_id'].iloc[0][:8]
            os.makedirs(self.output_dir, exist_ok=True)
            csv_path = os.path.join(self.output_dir, f"df_fusion_{user_room}_{garmin_id}_{datetime.now().strftime('%H%M%S')}.csv")
            df_final.to_csv(csv_path, index=False)
            log("ORCH_SYNC", f"CSV written on: {csv_path}")
    
            # self._apply_focus_engine(df_final) # COMMENTATO PER TEST
            # ---------------------------------------
        except Exception as e:
            log("ORCH_SYNC", f"Error in ambient synchronization: {e}")


    def process_wearable_batch(self, msgs):
        """
        Process a batch of wearable data messages.
        Args:
            msgs: List of Kafka message objects
        """
        all_metrics_list = []
        
        for msg in msgs:
            try:
                raw_data = json.loads(msg.value().decode('utf-8'))
                summary_type = raw_data.get('summary_type')
                garmin_id = raw_data.get('garmin_id')
                user_room = raw_data.get('user_room')

                if not garmin_id:
                    log("ORCH_PROC", "Skipping wearable message without garmin_id")
                    continue
                if self.allowed_garmin_ids and garmin_id not in self.allowed_garmin_ids:
                    log("ORCH_PROC", f"Skipping wearable message for non-allowed garmin_id: {garmin_id}")
                    continue

                for entry in raw_data.get('data', []):
                    t_start_unix = entry.get('startTimeInSeconds')
                    offset = entry.get('startTimeOffsetInSeconds', 0)
                    
                    # Inner helper function to compute local datetime
                    def _get_date_time(off):
                        ts_utc = datetime.fromtimestamp(t_start_unix + int(off), tz=timezone.utc)
                        return ts_utc.astimezone(CET)
                    
                match summary_type:
                    case 'epochs':
                        # every 15 minutes
                        dt = _get_date_time(0)
                        is_working = self._is_working_hour(dt)
                        if is_working:
                            all_metrics_list.append({
                                "timestamp_local": dt, "garmin_id": garmin_id, "room": user_room,
                                "activity_type": entry.get('activityType'),
                                "intensity": entry.get('intensity'),
                                "steps": entry.get('steps')
                            })

                    case 'stressDetails':
                        # every 3 minutes
                        stress_samples = entry.get('timeOffsetStressLevelValues', {})
                        for t_off, val in stress_samples.items():
                            dt = _get_date_time(t_off)
                            is_working = self._is_working_hour(dt)
                            if is_working and val >= 0: # -1/-2 are errors/missing data
                                all_metrics_list.append({
                                    "timestamp_local": dt, "garmin_id": garmin_id, "room": user_room,
                                    "stress_score": val
                                })
                        # Extract BODY BATTERY (usually same offsets as stress)
                        bb_samples = entry.get('timeOffsetBodyBatteryValues', {})
                        for t_off, val in bb_samples.items():
                            dt = _get_date_time(t_off)
                            is_working = self._is_working_hour(dt)
                            if is_working:
                                all_metrics_list.append({
                                    "timestamp_local": dt, "garmin_id": garmin_id, "room": user_room,
                                    "body_battery": val
                                })

                    case 'dailies':
                        # every 15 minutes
                        hr_samples = entry.get('timeOffsetHeartRateSamples', {})
                        for t_off, val in hr_samples.items():
                            dt = _get_date_time(t_off)
                            is_working = self._is_working_hour(dt)
                            if is_working:
                                all_metrics_list.append({
                                    "timestamp_local": dt, "garmin_id": garmin_id, "room": user_room,
                                    "hr": val
                                })

                    case 'hrv':
                        # every 5 minutes
                        hrv_samples = entry.get('hrvValues', {})
                        for t_off, val in hrv_samples.items():
                            dt = _get_date_time(t_off)
                            is_working = self._is_working_hour(dt)
                            # HRV è fondamentale sia per la baseline che per lo studio
                            if is_working and val is not None:
                                all_metrics_list.append({
                                    "timestamp_local": dt, 
                                    "garmin_id": garmin_id, 
                                    "room": user_room,
                                    "hrv": val
                                })

                    case 'allDayRespiration':
                        # every minute
                        resp_samples = entry.get('timeOffsetEpochToBreaths', {})
                        for t_off, val in resp_samples.items():
                            dt = _get_date_time(t_off)
                            is_working = self._is_working_hour(dt)
                            if is_working:
                                all_metrics_list.append({
                                    "timestamp_local": dt, 
                                    "garmin_id": garmin_id, 
                                    "room": user_room,
                                    "respiration_rate": val
                                })

                    case 'sleeps':
                            sleep_metrics_list = []
                            resp_samples = entry.get('timeOffsetSleepRespiration', {})
                            for t_off, val in resp_samples.items():
                                dt = _get_date_time(t_off)
                                # Sleep data is usually at night, so we consider all data as baseline
                                sleep_metrics_list.append({
                                    "timestamp_local": dt, "garmin_id": garmin_id, "room": user_room,
                                    "sleep_respiration_rate": val,
                                })
                                # Other sleep summary metrics
                                extra_sleep_metadata = {
                                    "total_sleep_duration": entry.get('durationInSeconds'),
                                    "deep_sleep_duration": entry.get('deepSleepDurationInSeconds'),
                                    "sleep_score": entry.get('overallSleepScore', {}).get('value')
                                }

                            if sleep_metrics_list:
                                df_sleep = pd.DataFrame(sleep_metrics_list)
                                # include into df also extra sleep metadata
                                for key, value in extra_sleep_metadata.items():
                                    df_sleep[key] = value
                                try:
                                    self.mongo_db["sleep_metrics_collection"].insert_many(df_sleep.to_dict('records'))
                                    log("ORCH_SLEEP", f"Saved {len(df_sleep)} sleep rows to MongoDB 'sleep_metrics_collection'.")
                                    os.makedirs(self.output_dir, exist_ok=True)
                                    csv_path = os.path.join(self.output_dir, f"df_sleep_{garmin_id}_{datetime.now().strftime('%H%M%S')}.csv")
                                    df_sleep.to_csv(csv_path, index=False)
                                    log("ORCH_SLEEP", f"Sleep CSV written on: {csv_path}")
                                except Exception as e:
                                    log("ORCH_SLEEP", f"Mongo insert failed for sleep data: {e}")
                    case _:
                        log("ORCH_PROC", f"Unknown summary_type '{summary_type}' in wearable data message.")

            except Exception as e:
                log("ORCH_PROC", f"Error parsing message in batch: {e}")

        if all_metrics_list:
            df_raw = pd.DataFrame(all_metrics_list)
            
            # TEMPORAL NORMALIZATION TO THE MINUTE
            # Necessary because HR samples every 15s, Stress every 180s.
            df_raw['timestamp_local'] = pd.to_datetime(df_raw['timestamp_local']).dt.floor('min')
            
            # MERGING (PIVOTING)
            # Group by minute, user, and room. For each minute, keep the first valid entry per metric.
            df_merged = df_raw.groupby(['garmin_id', 'room'])

            for (g_id, r_name), df_user_context in df_merged:
                log("ORCH_PROC", f"Processing context for User {g_id} in Room {r_name} ({len(df_user_context)} rows)")
                # Metrcs pivotingof the single user-room context   
                df_user_merged = df_user_context.groupby('timestamp_local').first().reset_index()
                # Synchronization with ambient data
                self._sync_with_ambient_data(df_user_merged, r_name)
    
# ------------------------------------------------------ ORCHESTRATOR RUN LOOP --------------------------------------------------------
    
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
                        log("ORCH_CONSUMER", f"Error: {msg.error()}")
                        continue
                match msg.topic():
                    case "user_presence":
                        self._handle_presence_update(msg) # OK
                    case "wearable_data" | "sensors_data":
                        self._enrich_and_trigger(msg) # OK
                    case "wearable_enriched_data":
                        self.wearable_buffer.append(msg) # OK
                        if len(self.wearable_buffer) >= self.BATCH_SIZE: # Process batch of 5 messages
                            self.process_wearable_batch(self.wearable_buffer)
                            self.wearable_buffer = []
                    case _:
                        log("ORCH_CONSUMER", f"Received message on unknown topic '{msg.topic()}'")

                if not self.kafka_auto_commit:
                    messages_count += 1

                if messages_count >= self.BATCH_SIZE:
                    self.consumer.commit(asynchronous=False)
                    log("ORCH_CONSUMER", f"Committed batch of {messages_count} messages.")
                    messages_count = 0

            # Final commit on shutdown
            if not self.kafka_auto_commit and messages_count > 0:
                self.consumer.commit(asynchronous=False)
                log("ORCH_CONSUMER", f"Final commit of {messages_count} messages on shutdown.")
    
        except Exception as e:
            log("ORCH_CONSUMER", f"Exception occurred: {str(e)}")
        finally:
            self.close()

    def setup(self):
        """Setup orchestrator components."""
        self._redis_setup()
        self._mongo_setup()
        self._setup_kafka_consumer()
        self._setup_kafka_producer()

    def close(self):
        """Close all connections and cleanup resources."""
        if not self.running:
            # Already closed, prevent double-closing
            return
        self.running = False

        if self.wearable_buffer:
            log("ORCH_SHUTDOWN", f"Processing remaining {len(self.wearable_buffer)} wearable messages before exit.")
            self.process_wearable_batch(self.wearable_buffer)
            self.wearable_buffer = []

        if self.consumer:
            try:
                self.consumer.close()
                log("ORCHESTRATOR", "Consumer closed.")
            except Exception as e:
                log("ORCHESTRATOR", f"Error closing consumer: {e}")
        if self.mongo_client:
            try:
                self.mongo_client.close()
                log("MONGO", "MongoDB connection closed.")
            except Exception as e:
                log("MONGO", f"Error closing MongoDB: {e}")
        if self.redis_client:
            try:
                self.redis_client.close()
                log("REDIS", "Redis connection closed.")
            except Exception as e:
                log("REDIS", f"Error closing Redis: {e}")
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                log("ORCHESTRATOR", "Producer flushed.")
            except Exception as e:
                log("ORCHESTRATOR", f"Error flushing producer: {e}")
        log("ORCHESTRATOR", "Exiting.")

if __name__ == "__main__":
    orchestrator = Orchestrator()
    try:
        orchestrator.setup()
        orchestrator.run()
    except Exception as e:
        log("CRITICAL", f"Orchestrator failed during execution: {e}")
        orchestrator.close()