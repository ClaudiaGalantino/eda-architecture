from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
import json, time, os, board, busio, sys, adafruit_dht, adafruit_sgp30

load_dotenv()

# connect to the broker - in this case it's the VM IP
mqtt_broker = os.getenv('MQTT_BROKER')
mqtt_port = int(os.getenv('PORT'))
mqtt_topic = os.getenv('TOPIC')
mqtt_client = os.getenv('MQTT_CLIENT')
room = os.getenv('ROOM')

sample_frequency = int(os.getenv('SAMPLE_FREQUENCY'))

# Validate environment variables
missing = []
for var_name, var_value in {
    'MQTT_BROKER': mqtt_broker,
    'MQTT_TOPIC': mqtt_topic,
    'MQTT_CLIENT': mqtt_client,
    'ROOM': room,
    'SAMPLE_FREQUENCY': sample_frequency,
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
        CET = ZoneInfo('Europe/Rome')
        current_time = datetime.now(CET).strftime("%Y-%m-%d %H:%M:%S")

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
            "room_name": room,
            "temperature_DHT22_C": dht_temp,
            "humidity_DHT22_%": dht_humidity,
            "CO2_ppm": co2,
            "TVOC_ppb": tvoc,
            "mqtt_topic": mqtt_topic,
        } 
        
        client.publish(mqtt_topic, json.dumps(data))
        print(f"Publishing {data} on topic: {mqtt_topic}")

        time.sleep(60 * sample_frequency)

except KeyboardInterrupt:
    print("Interrupted by user. Shutting down publisher...")
    client.loop_stop()
    client.disconnect()
