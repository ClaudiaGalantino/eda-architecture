# 🧩 Event-Driven Architecture (EDA) for Real-Time Data Processing
The project proposes the design and development of a software pipeline for the multimodal integration of data from environmental sensors and wearable devices, with the goal of building a scalable system for continuous and personalized monitoring of users in shared environments.

The architecture collects and synchronizes heterogeneous data streams, including physiological signals (heart rate, galvanic skin response, skin temperature) from wearable devices and environmental measurements (temperature, humidity, CO₂, noise, illuminance) from distributed room sensors. Each environment can host multiple users who share the same environmental measurements while maintaining independent physiological signals.

The system is based on an event-driven architecture that enables automatic data acquisition and real-time processing, reducing manual intervention and ensuring modularity and scalability across multiple environments.

---

This repository contains a **Docker-based Event-Driven Architecture (EDA)** for multimodal, real-time data collection and processing, combining:

- Environmental sensor streams (e.g., Raspberry Pi + DHT22/SGP30) published via MQTT

- Wearable data streams (Garmin devices) acquired through a Flask backend (OAuth + webhook notifications)

All streams are transported through Kafka topics, persisted to MongoDB, and can be further processed by an orchestrator component for enriched analytics (e.g., stress index).


## 🚀 Architecture Overview
The system follows a publish-subscribe paradigm across three specialized pipelines.

### Environmental Sensors Pipeline (MQTT → Kafka → MongoDB)

```text
[Raspberry Pi / Sensors] → [Mosquitto (MQTT)] → [MQTT→Kafka Bridge] → [Kafka] → [Kafka Consumer] → [MongoDB]
```

### Wearable Pipeline (Garmin → Backend → Kafka → MongoDB)

```text
[Garmin Connect] → [Backend (Flask OAuth + Webhook)] → [Kafka Producer] → [Kafka] → [Kafka Consumer] → [MongoDB]
```

### Presence / Orchestration Pipeline (CLI → Kafka → Redis/Mongo + Kafka)

```text
[User Presence Producer] → [Kafka topic: user_presence] → [Kafka Orchestrator] → [Redis Cache / Enriched Kafka topics]
```

> A system diagram is available in [`docs/architecture.md`](docs/architecture.md) (Mermaid).

## 🛠️ Main Components

| Component           |Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| Mosquitto           | MQTT broker receiving raw sensor data from IoT devices.                    |
| MQTT-Kafka Bridge   | Bridges the IoT world to the streaming world by forwarding MQTT messages to Kafka. |
| Backend Service     | Flask-based REST API handling Garmin OAuth flow and webhook notifications.  |
| Kafka Broker        | The central nervous system; manages all event streams and persistence.      |
| Kafka Consumer      | Multi-topic consumer that persists data into MongoDB based on specific mappings. |
| Orchestrator        | The "intelligence" layer. Correlates presence with sensor data and uses Redis for hot-caching. |
| MongoDB / Express   | NoSQL storage for long-term data analysis + a Web UI for data visualization. |
| FIT Processor       | Java-based service specifically for decoding Garmin's binary FIT file format. |
| Redis               | High-performance cache for storing real-time user presence and "current" environmental states. |


## 🧱 Repository Structure

```text
eda-architecture/
├── docker-compose.yml
├── env.example
├── configs/                      # Configuration files (Mosquitto, etc.)
├── docs/
├── edge/                         # Raspberry Pi sensor publisher script
├── services/
│   ├── backend/                  # Flask REST API + Garmin OAuth + Kafka producer
│   ├── fit-processor/            # Java-based FIT processor
│   ├── kafka_consumer/           # Kafka consumer → MongoDB writer
│   ├── kafka_orchestrator/       # Intelligent orchestration + Redis/Mongo + enriched topics
│   ├── mqtt_kafka_bridge/        # MQTT subscriber → Kafka publisher
│   └── user_presence_prod/       # Kafka producer for user presence events
└── data_analysis/                # Offline Jupyter notebooks for data exploration
    └── notebooks/                
```


## ⚙️ Technologies Used

- 🐳 Docker & Docker Compose
- 🧠 Apache Kafka (via `apache/kafka`)
- 🔌 Eclipse Mosquitto (MQTT)
- 🐍 Python (services run on Python 3.11/3.12 slim images)
  - `confluent-kafka`
  - `Flask` + `gunicorn` + `gevent`
  - `paho-mqtt`
  - `pymongo`
  - `python-dotenv`
  - `redis`, `pandas` (orchestrator)
- 🗄️ MongoDB 4.4 + 🌐 Mongo Express
- ☕ Java 17 (FIT processor)
- 🔐 SQLite (token + user mapping storage shared via Docker volume)
- 🌍 ngrok (optional external exposure for webhooks)


## 🔧 Setup & Configuration (.env files)

### 1. Clone the Repository
```bash
  git clone https://github.com/ClaudiaGalantino/eda-architecture.git
  cd eda-architecture
```

### 2. Root-level `.env` (Database & Ngrok)
Copy:

```bash
cp env.example .env
```

### 3. Service-level `.env` files
Create a `.env` file for each service from its `env.example`:

```bash
cp services/mqtt_kafka_bridge/env.example services/mqtt_kafka_bridge/.env
cp services/backend/env.example services/backend/.env
cp services/kafka_consumer/env.example services/kafka_consumer/.env
cp services/kafka_orchestrator/env.example services/kafka_orchestrator/.env
# user_presence uses the orchestrator env in docker-compose by default:
# services/user_presence_prod reads ./services/kafka_orchestrator/.env (see docker-compose.yml)
```

## ▶️ Deployment

```bash
docker compose up -d
```

Check containers:

```bash
docker ps
```

Follow logs (examples):

```bash
docker logs mosquitto -f
docker logs broker -f
docker logs mqtt_subscriber -f
docker logs backend -f
docker logs kafka_consumer -f
docker logs orchestrator -f
docker logs user_presence -f
docker logs mongo -f
docker logs mongo_express -f
docker logs cache -f
docker logs ngrok -f
```


## 🌐 Access endpoints

- **Backend API**: http://localhost:5000
  - Garmin OAuth UI: http://localhost:5000/
  - Garmin webhook endpoint: http://localhost:5000/garmin/webhook
- **Mongo Express**: http://localhost:8081
- **ngrok console** (if enabled): http://localhost:4040


## 🧪 Edge (Raspberry Pi publisher)

See [`edge/README.md`](edge/README.md).

The `edge/` folder contains:
- `publisher.py` (basic publisher)
- `publisher_extended.py` (extended publisher)
- `env.example` and `requirements.txt`


## 📊 Data analysis (offline)

The `data_analysis/` folder contains:
- `notebooks/` (Jupyter notebooks)
- `requirements.txt` (analysis dependencies)
- `env.example` (Mongo + CSV paths)

This part is intended for post-processing and exploration, separate from the real-time pipeline.