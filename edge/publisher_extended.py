# This script reads data from DHT22 and SGP30 sensors connected to a Raspberry Pi,
# and publishes the data to an MQTT broker at regular intervals.

# This publisher implementation, listens for control messages on a separate MQTT topic
# to start and stop the data publishing process.

# This allows remote control over when the sensor data is being collected and published.
# NOTE: works only if the Raspberry Pi is connected to the internet and it is always on.

from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import json, time, os, sys, threading, board, busio, adafruit_dht, adafruit_sgp30

load_dotenv()
CET = ZoneInfo("Europe/Rome")

def log(prefix, message):
    print(f"[{datetime.now(CET).strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

# connect to the broker - in this case it's the VM IP
mqtt_broker = os.getenv('MQTT_BROKER')
mqtt_port = int(os.getenv('PORT'))
mqtt_topic = os.getenv('TOPIC')
mqtt_client = os.getenv('MQTT_CLIENT')
room = os.getenv('ROOM')
control_topic = os.getenv('MQTT_SUB_TOPIC')
control_client_id = os.getenv('MQTT_SUB_CLIENT')

sample_frequency = int(os.getenv('SAMPLE_FREQUENCY'))

# Validate environment variables
missing = []
for var_name, var_value in {
    'MQTT_BROKER': mqtt_broker,
    'MQTT_TOPIC': mqtt_topic,
    'MQTT_CLIENT': mqtt_client,
    'ROOM': room,
    'SAMPLE_FREQUENCY': sample_frequency,
    'MQTT_SUB_TOPIC': control_topic,
    
}.items():
    if not var_value:
        missing.append(var_name)
        sys.exit(1)

# Create an MQTT client to subscribe to control messages
client = mqtt.Client(
    client_id=control_client_id,
    protocol=mqtt.MQTTv311,
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)    

i2c = None
sgp30 = None
dhtDevice = None
try:
    # define the senors 
    i2c = busio.I2C(board.SCL, board.SDA)
    sgp30 = adafruit_sgp30.Adafruit_SGP30(i2c)
    dhtDevice = adafruit_dht.DHT22(board.D4)
except Exception as e:
    log("SENSOR_INIT", f"Error initializing sensors: {e}")

# Global flag to control sampling
sampling_active = False

def publish_sensor_data():
    """
    Publish sensor data to the MQTT broker.
    """
    global sampling_active
    while sampling_active:
        try:
        # sensor initialization and calibration of SGP30
            log("SYSTEM_PUBLISHER", "Starting the sgp30")
            for i in range(60):
                if not sampling_active: return
                try:
                    sgp30.eCO2
                    sgp30.TVOC
                except OSError as e:
                    log("I2C_ERROR", f"Error reading for second {i}: {e}. Trying to continue...")
                except Exception as e:
                    log("SYSTEM_PUBLISHER", f"SGP30 initialization error: {e}")

                if i % 10 == 0:
                        log("SYSTEM_PUBLISHER", f"Calibration in progress: {i}/60s")
                time.sleep(1)
            while sampling_active:
                try: 
                    current_time = datetime.now(CET).strftime("%Y-%m-%d %H:%M:%S")
                    # data collection
                    try:
                        dht_temp = dhtDevice.temperature
                        dht_humidity = dhtDevice.humidity
                    except Exception as e:
                        print(f"DHT22 read error: {e}")
                        dht_temp = None
                        dht_humidity = None
                        time.sleep(2)
                    try:
                        co2 = sgp30.eCO2
                        tvoc = sgp30.TVOC
                    except Exception as e:
                        print(f"SGP30 read error: {e}")
                        co2 = None
                        tvoc = None

                    data = {
                        "timestamp": current_time,
                        "room_name": room,
                        "temperature_DHT22_C": dht_temp,
                        "humidity_DHT22_%": dht_humidity,
                        "CO2_ppm": co2,
                        "TVOC_ppb": tvoc,
                        "mqtt_topic": mqtt_topic,
                    } 
                    
                    client.publish(mqtt_topic, json.dumps(data))
                    print(f"Publishing {data} on topic: {mqtt_topic}")

                    for _ in range(sample_frequency * 60):
                        if not sampling_active: 
                            break
                        time.sleep(1)

                except KeyboardInterrupt:
                    print("Interrupted by user. Shutting down publisher...")
        except Exception as e:
            log("THREAD_CRITICAL", f"Errore imprevisto nel thread: {e}")
            time.sleep(5) # Aspetta prima di riavviare il loop interno


def on_connect(client_sub, userdata, flags, rc, properties=None):
    """
    Callback for MQTT connection.
    Args:
        client_sub: The MQTT client instance.
        userdata: The private user data.
        flags: Response flags sent by the broker.
        rc: The connection result.
        properties: MQTT v5.0 properties.
    """
    if rc == 0:
        log("MQTT_RASPI", "Connected to MQTT Broker!")
        client.subscribe(control_topic)
    else:
        log("MQTT_RASPI", f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    """
    Callback for MQTT message reception.
    Args:
        client: The MQTT client instance.
        userdata: The private user data.
        msg: The received MQTT message.
    """
    global sampling_active
    payload = msg.payload.decode('utf-8', errors='replace')
    log("MQTT", f"Message received on topic {msg.topic}: {payload}")
    try:
        if msg.topic == control_topic:
            data_received = json.loads(payload)
            command = str(data_received.get('command', '')).strip().lower()

            if command == 'start':
                log("MQTT", "Start command received.")
                if not sampling_active:
                    sampling_active = True
                    threading.Thread(target=publish_sensor_data, daemon=True).start()
                else: 
                    log("MQTT", "Sampling is already active.")
            elif command == 'stop':
                log("MQTT", "Stop command received.")
                sampling_active = False
    except json.JSONDecodeError:
        log("MQTT", f"Error processing message: {payload}")
        return
    
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(mqtt_broker, mqtt_port)
    client.loop_forever()
except Exception as e:
    log("SYSTEM", f"Error occurred: {e}")
    
finally:
    sampling_active = False
    client.disconnect()
    log("SYSTEM", "MQTT producer and subscriber stopped.")
