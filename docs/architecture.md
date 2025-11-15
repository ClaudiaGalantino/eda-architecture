# ☁️ System Architecture Diagram

This diagram illustrates the complete data flow within the **Event-Driven Architecture (EDA)** system,  
from IoT room nodes and wearables up to cloud ingestion, processing, and federated learning components.

```mermaid
flowchart TD
  subgraph SENSING_UNIT
    subgraph WEARABLES
          W["Wearables (Garmin)"]
          APP["Garmin Connect"]
          SDK["Garmin SDK / API"]
      end
    subgraph ROOM_NODES [Room Nodes]
        S_A["Env sensors (Room A)"]
        S_B["Env sensors (Room B)"]
        S_C["Env sensors (Room C)"]
    end
    
  end

  subgraph CLOUD [Cloud]
    subgraph Ingestion
		    M1["MQTT publisher"]
        M["Mosquitto"]
        M2[""MQTT-Kafka bridge]
        PROD["Kafka Producer"]
    end
    subgraph Processing
		    
        STREAM["Kafka Broker"]
        CONSUMER["Kafka Consumer"]
        ORCH["Intelligent Orchestrator"]
        FL_COORD["Federated Learning"]
    end
    STORE["Model & Data Storage (Cloud Storage)"]
    IDX1["Stress Index"]
    IDX2["Alert Index"]
    IDX3["Recovery Index"]
  end

  %% =====================
  %% CONNECTIONS
  %% =====================
  %% Room nodes push
  S_A & S_B & S_C --> M1
  M1-->M-->M2
  M2 --> STREAM

  %% Orchestrator invokes wearable producer
  PROD --> STREAM
  ORCH --> SDK
  SDK --> PROD

  %% Consumer DB
  STREAM --> CONSUMER
  CONSUMER --> STORE

  %% Orchestrator reads raw data from the DB to compute indices
  ORCH <--> CONSUMER
  ORCH --> IDX1 & IDX2 & IDX3

  %% Federated learning
  FL_COORD <--> STORE 
  ORCH <--> FL_COORD
```

## 🧭 Description

Room Nodes: Environmental sensors deployed in multiple rooms publish data via MQTT.

Wearables: Garmin devices stream biometric data through the Garmin Connect SDK.

MQTT Broker: Acts as the ingestion layer for IoT messages.

Kafka Broker: Processes incoming data streams and distributes them to downstream services.

Intelligent Orchestrator: Performs analytics and computes key indices (Stress, Alert, Recovery).

Federated Learning Coordinator: Manages model updates across distributed nodes while preserving data privacy.

Cloud Storage: Central repository for models and processed data.