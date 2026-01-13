# 🧩 Event-Driven Architecture (EDA) for Real-Time Data Processing

The project proposes the design and development of a software pipeline for the multimodal integration of data from environmental sensors and wearable devices, with the goal of building a scalable system for continuous and personalized monitoring of users in shared environments.

The architecture collects and synchronizes heterogeneous streams — physiological data (heart rate, galvanic skin response, skin temperature) from wearable devices, and environmental data (temperature, humidity, CO₂, noise, illuminance) from distributed room sensors. Each environment can host multiple users who share the same environmental measurements but have independent physiological signals.

The system is based on an event-driven architecture that enables automatic acquisition and real-time processing, reducing manual intervention and ensuring modularity and scalability across multiple environments.

## 🚀 Architecture

The architecture follows a **publish-subscribe** paradigm with two main data pipelines:

**Environmental Sensors Pipeline:**
```
[IoT Sensors/Raspberry Pi] → [MQTT Broker] → [MQTT-Kafka Bridge] → [Kafka Broker] → [Kafka Consumer] → [MongoDB]
```

**Wearable Data Pipeline:**
```
[Garmin Devices] → [Garmin Connect API] → [Backend Webhook] → [Kafka Producer] → [Kafka Broker] → [Kafka Consumer] → [MongoDB]
```

### Data Flow

1. **Environmental sensors** (DHT22, SGP30) on Raspberry Pi nodes publish JSON data to MQTT topics
2. **MQTT-Kafka Bridge** subscribes to MQTT topics and forwards messages to Kafka topic `sensors_data`
3. **Garmin wearables** trigger webhook notifications to the Backend API when new data is available
4. **Backend service** fetches wearable data via Garmin OAuth API and publishes to Kafka topic `wearable_data`
5. **Kafka Consumer** subscribes to both topics and persists data to corresponding MongoDB collections
6. **MongoDB** stores all data with configurable collection mappings per topic

### Main Components

| Component | Description |
|------------|-------------|
| **MQTT Broker (Mosquitto)** | Receives messages from IoT devices (e.g., Raspberry Pi). |
| **MQTT-Kafka Bridge** | Subscribes to MQTT topics and publishes sensors data to Kafka topics. |
| **Backend Service** | Flask-based REST API handling Garmin OAuth flow and webhook notifications. Includes embedded Kafka producer for wearable data. |
| **Kafka Broker** | Apache Kafka message broker managing event streams. |
| **Kafka Consumer** | Consumes messages from Kafka topics and persists data to MongoDB collections based on configurable topic-to-collection mappings. |
| **MongoDB** | NoSQL database storing processed sensor and wearable data. |
| **Mongo Express**	| Web UI for MongoDB visualization and management |
| **FIT Processor** | Java-based service for processing Garmin FIT files. |

## 🧱 Project Structure
Top-level layout of the main folders and files in this repository:
```
eda-architecture/
│
├── docker-compose.yml           
├── env.example                   
├── configs/
│   └── mosquitto.conf                        
├── docs/
│   └── architecture.md          
├── edge/                        # Raspberry Pi sensor publisher
│   ├── env.example
│   ├── publisher.py             
│   ├── requirements.txt
│   └── README.md
├── services/
│   ├── backend/                 # Flask REST API + Garmin OAuth + Kafka producer
│   │   ├── Dockerfile
│   │   ├── wsgi.py              # Gunicorn entry point
│   │   ├── requirements.txt
│   │   ├── env.example
│   │   └── app/
│   │       ├── routes/          # OAuth and Garmin webhook routes
│   │       ├── processing/      # Wearable data producer & processor
│   │       ├── db_utils.py      # SQLite token & user mapping utilities
│   │       └── garmin_client.py # Garmin OAuth 1.0a client
│   ├── fit-processor/           # Java-based FIT file processor
│   │   └── Dockerfile
│   ├── kafka_consumer/          # Kafka consumer -> MongoDB writer
│   │   ├── Dockerfile
│   │   ├── kafka_consumer.py
│   │   ├── env.example
│   │   └── requirements.txt
│   └── mqtt_kafka_bridge/       # MQTT subscriber -> Kafka publisher
│       ├── Dockerfile
│       ├── mqtt_kafka_bridge.py
│       ├── env.example
│       └── requirements.txt
├── .gitignore
└── README.md
```

## ⚙️ Technologies Used

- 🐳 **Docker & Docker Compose** – container orchestration  
- 🧠 **Apache Kafka** – message streaming and event management  
- 🔌 **Eclipse Mosquitto (MQTT Broker)** – lightweight IoT communication  
- 🐍 **Python 3.11-3.12** – application logic
  - `paho-mqtt` – MQTT client
  - `confluent-kafka` – Kafka client
  - `Flask` – REST API framework
  - `Gunicorn` with `gevent` – production WSGI server
  - `requests-oauthlib` – OAuth 1.0a for Garmin API
- 🗄️ **MongoDB 4.4** – NoSQL database for storing sensor and wearable data
- 🌐 **Mongo Express** – web-based MongoDB admin interface
- ☕ **Java 17** – FIT file processing service
- 🔐 **SQLite** – local storage for OAuth tokens and user mappings
- 🌱 **python-dotenv** – environment variable management 

## ▶️ How to Run the Project

### 1. Clone the Repository
```bash
  git clone https://github.com/ClaudiaGalantino/eda-architecture.git
  cd eda-architecture
```
### 2. Configure Environment Variables
Copy and customize the example `.env` files provided in the service folders:

```bash
# Root-level MongoDB and Mongo Express configuration
cp env.example .env

# Service-specific configurations
cp services/mqtt_kafka_bridge/env.example services/mqtt_kafka_bridge/.env
cp services/kafka_consumer/env.example services/kafka_consumer/.env
cp services/backend/env.example services/backend/.env
```

**Key configuration points:**

- **Root `.env`**: MongoDB credentials and Mongo Express settings
- **`mqtt_kafka_bridge/.env`**: MQTT broker address, topics, and Kafka target topic
- **`kafka_consumer/.env`**: Kafka topics to consume, MongoDB connection, and topic-to-collection mappings
- **`backend/.env`**: Garmin API credentials, Kafka broker, callback URLs, and Flask secret key

Ensure all broker addresses use service names (e.g., `broker:9092`, `mosquitto:1883`) for Docker network communication.

### 3. Start the Containers
```bash
docker compose up -d
```
All services will start within a dedicated Docker network defined in docker-compose.yml.

### 4. Check the Logs
Monitor individual service logs to verify proper operation:

```bash
# View all running containers
docker ps

# Follow logs for specific services
docker logs mosquitto -f           # MQTT broker
docker logs broker -f              # Kafka broker
docker logs mqtt_subscriber -f     # MQTT-Kafka bridge
docker logs backend -f             # Flask API + Garmin OAuth
docker logs kafka_consumer -f      # Kafka consumer
docker logs mongo -f               # MongoDB
docker logs mongo_express -f       # MongoDB web UI
```

### 5. Access Web Interfaces

- **Backend API**: http://localhost:5000
  - OAuth flow: http://localhost:5000/login
  - Garmin webhook endpoint: http://localhost:5000/garmin/webhook
- **Mongo Express**: http://localhost:8081 (MongoDB admin interface)

