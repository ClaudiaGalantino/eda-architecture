# ☁️ System Architecture Diagram

```mermaid
flowchart TD
%% ============================================================
%% PHYSICAL LAYER
%% ============================================================
subgraph PHYSICAL_LAYER [PHYSICAL LAYER]
    direction TB
    
    subgraph C [C - WEARABLE NODE]
        W1[Garmin device_1]
        W2[Garmin device_2]
        G_APP[Garmin Connect]
        W1 & W2 --> G_APP
    end

    subgraph A [A - SENSING UNITS]
        S_A["Env sensors (Room A)
        MQTT Publisher"]
        S_B["Env sensors (Room B)
        MQTT Publisher"]
        S_C["Env sensors (Room C)
        MQTT Publisher"]
    end
end

%% ============================================================
%% DOCKER CONTAINERS
%% ============================================================
subgraph DOCKER [DOCKER CONTAINERS]

    subgraph H [H - USER INTERACTION]
        CLI["User Presence CLI
        Kafka Producer"]
    end

    subgraph PROCESSING [PROCESSING]
        D[D - Backend Flask]
        KAFKA{{"kafka"}}
        E[E - Kafka Standard Consumer]
        F[F - Intelligent Orchestrator]
        STRESS[Stress Index]
        
        D -- "Produce on topic 
        wearable_data" --> KAFKA
        CLI -- "Produce on
        user_presence" --> KAFKA
        KAFKA -- "consume data" --> E
        KAFKA -- "consume data" --> F
        F --> STRESS
    end

    subgraph B [B - DATA INGESTION]
        direction TB
        MOSQ["mosquitto"]
        BRIDGE["MQTT - Kafka bridge"]
        
        MOSQ -- "subscribe to all topics" --> BRIDGE
    end

    subgraph G [G - MONGO DB]
        DB[(mongo DB)]
    end

end

%% ============================================================
%% CONNECTIONS & FLOWS
%% ============================================================

%% Wearable flow
G_APP -- "Data" --> D

%% Sensing units flow
S_A -- "Publish data on topic 
sensor/room_A" --> MOSQ
S_B -- "Publish data on topic 
sensor/room_B" --> MOSQ
S_C -- "Publish data on topic 
sensor/room_C" --> MOSQ

%% Bridge to Kafka
BRIDGE -- "Produce on 
sensor_data" --> KAFKA

%% Storage connections
E --"store raw data"--> DB
STRESS --> DB
```


## 🧭 System Description
The architecture follows an Event-Driven Architecture pattern, leveraging Docker containers to decouple data ingestion, message streaming, and intelligent processing.

1. Physical Layer (Data Sources)

[A] Sensing Units: Environmental sensors located in different zones (Rooms A, B, and C). These act as MQTT Publishers, broadcasting telemetry data to specific room-based topics.

[C] Wearable Node: Garmin devices (1 & 2) that sync via Garmin Connect. This node handles the initial collection of biometric data before sending it to the cloud backend.

2. Data Ingestion & Interaction

[B] Data Ingestion: A centralized Mosquitto broker receives all environmental MQTT traffic. An MQTT-Kafka Bridge subscribes to these topics and transforms the messages into Kafka events, producing them on the `sensor_data` topic.

[H] User Interaction: A dedicated User Presence CLI acts as a Kafka Producer, allowing manual or automated injections of user status data into the `user_presence` topic.

[D] Backend Flask: Serves as the entry point for wearable data. It processes incoming requests from the Garmin ecosystem and produces events on the `wearable_data` Kafka topic.

3. Processing & Streaming Layer

Kafka Broker: The central nervous system of the architecture. It manages the pub/sub logic for three main data streams: wearable data, sensor data, and user presence.

[F] Intelligent Orchestrator: A specialized consumer that pulls data from Kafka to perform real-time analysis, resulting in the computation of the Stress Index.

[E] Kafka Standard Consumer: A utility service dedicated to data persistence. It consumes raw data streams from Kafka to ensure they are backed up.

4. Storage & Persistence

[G] MongoDB: The primary persistent storage layer. It stores Raw Data: Handled by the Standard Consumer [E].
Processed Indices: Calculated values (like the Stress Index) sent from the Orchestrator [F].