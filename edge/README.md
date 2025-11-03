# Raspberry Pi Sensor Publisher

This module is designed to collect environmental data from sensors connected to a Raspberry Pi and publish it to an MQTT broker as part of the thesis Event Driven Architecture (EDA) project.

## 🔍 Overview

The script reads data from the following sensors:

- **DHT22** – Temperature and humidity

- **SGP30** – CO₂ and TVOC air quality values

Instead of storing data locally as CSV files, it publishes sensor readings to an MQTT topic (e.g. room1) for downstream processing or cloud ingestion.

## ⚙️ Requirements

- Raspberry Pi (any model with GPIO and I²C support)

- Python >= 3.10

- Virtual environment recommended

- MQTT broker reachable from the Raspberry Pi (e.g. running in a Docker container on a VM)

## 🪄 Setup Instructions

- Clone the repository

```bash
git clone <your-repo-url>
cd edge
```


- Create and activate a virtual environment

```bash 
python3 -m venv <room_name>-env
source <room_name>/bin/activate
```


- Install dependencies
```bash
pip install -r requirements.txt
```

- Configure environment variables

Copy the provided example file and update the values according to your setup:

```bash
cp .env.example .env
```
```text
Example .env for room_name: lab

MQTT_BROKER=192.168.xx.xx
PORT=1883
TOPIC=sensor/lab
MQTT_CLIENT=lab-publisher
SAMPLE_FREQUENCY=10  # in minutes
```


## 🚀 Run the publisher

Once everything is configured, run the publisher script:

<python3 publisher.py>


It will:

- Connect to the MQTT broker defined in .env

- Collect sensor readings every SAMPLE_FREQUENCY minutes

- Publish the data as a single JSON message, e.g.

```json
{
  "time": "2025-10-16 17:41:43",
  "temperature": 24.3,
  "humidity": 51.0,
  "co2": 400,
  "tvoc": 0
}
```

## 🔐 Environment Files

The .env file contains local configuration (e.g., IP addresses, topics, credentials).

It must not be committed to Git — the repository includes .env.example for reference.

The .gitignore already excludes .env and virtual environment folders.

## 🧩 Notes

Ensure the MQTT broker is reachable from the Raspberry Pi (e.g., test with  
  ```bash
  nc -zv <BROKER_IP> 1883
  ```
).

Each Raspberry Pi or room can use its own topic (e.g. room1, room2) for modular data streams.

The script can be extended to add error handling, reconnection logic, or support for additional sensors