# 🧩 Event-Driven Architecture (EDA) for Real-Time Data Processing

The project proposes the design and development of a software pipeline for the multimodal integration of data from environmental sensors and wearable devices, with the goal of building a scalable system for continuous and personalized monitoring of users in shared environments.

The architecture collects and synchronizes heterogeneous streams — physiological data (heart rate, galvanic skin response, skin temperature) from wearable devices, and environmental data (temperature, humidity, CO₂, noise, illuminance) from distributed room sensors. Each environment can host multiple users who share the same environmental measurements but have independent physiological signals.

The system is based on an event-driven architecture that enables automatic acquisition and real-time processing, reducing manual intervention and ensuring modularity and scalability across multiple environments.

## 🚀 Architecture

The architecture follows a **publish-subscribe** paradigm and consists of:

[IoT Sensors / Wearables] → [MQTT Broker] → [MQTT-Kafka Bridge] → [Kafka Broker] → [Consumer Services] → [MongoDB Storage]

### Main Components

| Component | Description |
|------------|-------------|
| **MQTT Broker (Mosquitto)** | Receives messages from IoT devices (e.g., Raspberry Pi). |
| **MQTT-Kafka Bridge** | Subscribes to MQTT topics and publishes messages to Kafka topics. |
| **Wearable Producer**	| |
| **Kafka Broker** | Manages message queues and ensures reliable event delivery. |
| **Kafka Consumer** | Consumes messages from Kafka for processing or storage. |
| **MongoDB** | Stores processed data.|
| **Mongo Express**	| Web UI for MongoDB visualization and management.|

## 🧱 Project Structure
Top-level layout of the main folders and files in this repository:
```
eda-architecture/
│
├── docker-compose.yml
├── configs/
│   └── mosquitto.conf
├── docs/
│   └── architecture.md
├── edge/
│   ├── env.example
│   ├── mqtt_subscriber.py
│   ├── publisher.py
│   └── README.md
├── services/
│   ├── mqtt_kafka_bridge/
│   │   ├── mqtt_kafka_bridge.py
│   │   ├── env.example
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   ├── kafka_consumer/
│   │   ├── consumer.py
│   │   ├── env.example
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── wearable_producer/ 
│       ├── producer.py
│       ├── env.example
│       ├── requirements.txt
│       └── Dockerfile
├── .gitignore
└── README.md

```

## ⚙️ Technologies Used

- 🐳 **Docker & Docker Compose** – container orchestration  
- 🧠 **Apache Kafka** – message streaming and event management  
- 🔌 **Eclipse Mosquitto (MQTT Broker)** – lightweight IoT communication  
- 🐍 **Python** – application logic (using `paho-mqtt` and `kafka-python`)  
- 🗄️ MongoDB – storage backend
- 🌐 Mongo Express – visualization interface for MongoDB
- 🌱 dotenv – environment variable management 

## ▶️ How to Run the Project

### 1. Clone the Repository
```bash
  git clone https://github.com/ClaudiaGalantino/eda-architecture.git
  cd eda-architecture
```
### 2. Configure Environment Variables
Copy and customize the example .env files provided in the service folders:

```bash
cp services/mqtt_kafka_bridge/env.example services/mqtt_kafka_bridge/.env
cp services/kafka_consumer/env.example services/kafka_consumer/.env
cp services/wearable_producer/env.example services/wearable_producer/.env 
```
Then, update the MQTT and Kafka broker addresses to match your network.

### 3. Start the Containers
```bash
docker compose up -d
```
All services will start within a dedicated Docker network defined in docker-compose.yml.

### 4. Check the Logs
```bash
docker ps
docker logs mosquitto -f
docker logs broker -f
docker logs mqtt_subscriber -f
docker logs kafka_consumer -f
docker logs mongo -f
docker logs mongo_express -f
```
### 5. MongoDB Access Interface
Mongo Express: http://localhost:8081
