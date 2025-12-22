from datetime import datetime
from signal import signal
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import os, sys, json, signal, random, string, time

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
    Handle shutdown signals to gracefully stop the bridge.
    """
    global running
    log("SYSTEM", f"Shutdown signal {signum} received. Stopping...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)
random_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=4))

# connect to the broker
mqtt_broker = os.getenv('MQTT_BROKER')
mqtt_port = int(os.getenv('PORT'))
mqtt_topic = os.getenv('TOPIC')
mqtt_client = f"{os.getenv('MQTT_CLIENT_PROD_ID')}_{random_suffix}"

# Validate environment variables
missing = []
for var_name, var_value in {
    'MQTT_BROKER': mqtt_broker,
    'MQTT_TOPIC': mqtt_topic,
    'MQTT_CLIENT': mqtt_client
}.items():
    if not var_value:
        missing.append(var_name)
        sys.exit(1)

# Create an MQTT client with a specific client ID, using MQTT 3.1.1 protocol and the latest callback API
client = mqtt.Client(
    client_id=mqtt_client,
    protocol=mqtt.MQTTv311,
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)
client.connect(mqtt_broker, mqtt_port)
client.loop_start()

def publish_edge_command(command):
    """
    Publish an edge control command to the MQTT topic.
    The actual start/stop logic is implemented elsewhere.
    """
    try:
        data = {
            "command": command
        }

        client.publish(mqtt_topic, json.dumps(data))
        log("EDGE_INITIATION", f"Published data to topic {mqtt_topic}: {data}")

    except Exception as e:
        log("EDGE_INITIATION", f"Error publishing data: {e}")


def cli_interface():
    """
    Simple CLI to start or stop edge device.
    """
    log("EDGE_INITIATION", "CLI started. Type 'exit' to quit.")
    while running:
            try:
                command = input("Enter \"start\" to start edge devices (or 'stop' to quit): ")
                if command.lower() == 'exit':
                    break
                publish_edge_command(command)
            except KeyboardInterrupt:
                handle_shutdown(signal.SIGINT, None)
                break
            except Exception as e:
                log("CLI-ERR", f"Error during input: {e}")
                time.sleep(1)

    client.loop_stop()
    client.disconnect()
    log("SYSTEM", "Edge command CLI stopped.")

if __name__ == '__main__':
    cli_interface()