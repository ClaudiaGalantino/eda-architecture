from datetime import datetime
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import json
import time
import os
import board
import busio
import adafruit_dht
import adafruit_sgp30
import os
import sys

load_dotenv()

# connect to the broker - in this case it's the VM IP
mqttBroker = os.getenv('MQTT_BROKER')
port = int(os.getenv('PORT'))
topic = os.getenv('TOPIC')
mqtt_client = os.getenv('MQTT_CLIENT')

sample_frequency = int(os.getenv('SAMPLE_FREQUENCY'))

missing = []
if not mqttBroker:
    missing.append('MQTT_BROKER')

# ensure port is valid integer (port may already be int)
try:
    port = int(port)
except Exception:
    missing.append('PORT (missing or not an integer)')

if not topic:
    missing.append('TOPIC')

if not mqtt_client:
    missing.append('MQTT_CLIENT')

try:
    sample_frequency = int(sample_frequency)
except Exception:
    missing.append('SAMPLE_FREQUENCY (missing or not an integer)')

if missing:
    print(f"Missing or invalid environment variables: {', '.join(missing)}")
    sys.exit(1)

# Create an MQTT client with a specific client ID, using MQTT 3.1.1 protocol and the latest callback API (version 2)
client = mqtt.Client(client_id=mqtt_client, protocol=mqtt.MQTTv311, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.connect(mqttBroker, port)
client.loop_start()


# define the senors 
i2c = busio.I2C(board.SCL, board.SDA)
sgp30 = adafruit_sgp30.Adafruit_SGP30(i2c)
dhtDevice = adafruit_dht.DHT22(board.D4)

# sensor initialization and calibration of SGP30
print("Starting the sgp30")
for _ in range(60):
    sgp30.eCO2
    sgp30.TVOC
    time.sleep(1)

try: 
    while True:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # data collection
        try:
            dht_temp = dhtDevice.temperature
            dht_humidity = dhtDevice.humidity
        except Exception as e:
            print(f"DHT22 read error: {e}")
            dht_temp = None
            dht_humidity = None

        try:
            co2 = sgp30.eCO2
            tvoc = sgp30.TVOC
        except Exception as e:
            print(f"SGP30 read error: {e}")
            co2 = None
            tvoc = None

        data = {
            "timestamp": current_time,
            "temperature_DHT22_C": dht_temp,
            "humidity_DHT22_%": dht_humidity,
            "CO2_ppm": co2,
            "TVOC_ppb": tvoc,
        } 
        
        # publish data collected
        client.publish(topic, json.dumps(data))
        print(f"Publishing {data} on topic: {topic}")

        time.sleep(60 * sample_frequency)

except KeyboardInterrupt:
    print("Interrupted by user. Shutting down publisher...")
    client.loop_stop()
    client.disconnect()
