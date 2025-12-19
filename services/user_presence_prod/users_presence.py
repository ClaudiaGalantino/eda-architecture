from time import time
from confluent_kafka import Producer
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os, signal, sys, json

load_dotenv()

CET = ZoneInfo("Europe/Rome")
def log(prefix, message):
    """
    Simple logger function.
    """
    print(f"[{datetime.now(CET).strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

running = True
def handle_shutdown(signum, frame):
    """
    Handle shutdown signals to gracefully stop the producer.
    """
    global running
    log("SYSTEM", f"Shutdown signal {signum} received. Stopping...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# Kafka setup
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = os.getenv('KAFKA_TOPIC')
kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
kafka_acks = os.getenv('KAFKA_ACKS', 'all')
kafka_retries = int(os.getenv('KAFKA_RETRIES', 5))
rooms_list_raw = os.getenv('ROOMS_LIST')
users_email_list_raw = os.getenv('USER_EMAILS_LIST')

# Validate environment variables
missing = []
for var_name, var_value in {
    "KAFKA_BROKER": kafka_broker,
    "KAFKA_TOPIC": kafka_topic,
    "KAFKA_CLIENT_ID": kafka_client_id,
    "ROOMS_LIST": rooms_list_raw,
    "USER_EMAILS_LIST": users_email_list_raw,
}.items():
    if not var_value:
        missing.append(var_name)

if missing:
    message = f"Environment variable(s) {', '.join(missing)} are missing or invalid."
    log("SYSTEM - USER_PRESENCE", message)
    sys.exit(1)

# parse lists from env
try:
     rooms_list = [room.strip() for room in rooms_list_raw.split(',')]
     users_email_list = [email.strip() for email in users_email_list_raw.split(',')]
except Exception as e:
    log("SYSTEM - USER_PRESENCE", f"Error parsing lists: {e}")
    sys.exit(1)

# Kafka Producer setup
kafka_conf = {
    'bootstrap.servers': kafka_broker,
    'client.id': kafka_client_id,
    'acks': kafka_acks,
    'retries': kafka_retries
}
producer = Producer(kafka_conf)

def delivery_report(err, msg):
    """
    Callback for message delivery reports.
    """
    if err is not None:
        log("USER_PRESENCE", f"Message delivery failed: {err}")
    else:
        log("USER_PRESENCE", f"Message successfully delivered!")
        log("USER_PRESENCE", f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")

def send_presence_update(user_email, room_id, status):
    """
    Send a presence update message to Kafka.
    """
    current_time = datetime.now(CET).strftime("%Y-%m-%d %H:%M:%S")
    message = {
        "timestamp": current_time,
        "user_email": user_email,
        "room_id": room_id,
        "status": status
    }

    value_bytes = json.dumps(message, ensure_ascii=False).encode('utf-8')
    key_bytes = str(room_id).encode('utf-8')
    try:
        producer.produce(
            kafka_topic,
            key=key_bytes,
            value=value_bytes,
            callback=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        log("USER_PRESENCE", f"Failed to send message: {e}")

def cli_interface():
    """
    Simple CLI to input presence updates.
    """
    log("USER_PRESENCE", "CLI started. Type 'exit' to quit.")
    while running:
            try:
                producer.poll(0) 
                user_emails_input = input("User Email(s) [; separator]: ")

                if user_emails_input.lower() == 'exit':
                    break

                emails_to_process = [email.strip() for email in user_emails_input.split(';')]

                room_id = input("Room name: ")
                if room_id.lower() == 'exit': break
                if room_id not in rooms_list:
                    log("CLI-ERR", f"Room '{room_id}' not valid.")
                    continue

                status_input = input("Action [E]nter / [X]it: ").lower()
                if status_input not in ['e', 'x']:
                    log("CLI-ERR", "Invalid status. Use 'E' for Enter or 'X' for Exit.")
                    continue
                
                status = 'ENTER' if status_input == 'e' else 'EXIT'

                for email in emails_to_process:
                    send_presence_update(email, room_id, status)

            except KeyboardInterrupt:
                handle_shutdown(signal.SIGINT, None)
                break
            except Exception as e:
                log("CLI-ERR", f"Error during input: {e}")
                time.sleep(1)

    producer.flush()
    log("SYSTEM", "User Presence CLI stopped.")

if __name__ == '__main__':
    cli_interface()