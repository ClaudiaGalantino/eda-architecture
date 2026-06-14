# Study Data Analysis - Garmin Vìvoactive 5

This document outlines the steps taken to retrieve the metrics required for Table 1, using the MongoDB database running on Docker and analyzing the repository's codebase.

## 1. Participants and Duration
The study involved **5 participants** equipped with Garmin Vìvoactive 5 devices over a **30-day** period. Although the framework supports more users (21 mapped in the SQLite database), the final analysis focused on the active data generated during study sessions.


## 2. Data Collection Metrics

### 2.1 Wearable Events
- **Metric:** Total number of Kafka messages (batches) received and stored in the raw collection.
- **Manual Query:**
  ```bash
  docker exec mongo mongo -u root -p example --authenticationDatabase admin thesis_db --eval "db.wearable_data_collection.count()"
  ```
- **Result:** **4,900** wearable events.
- **Description:** Each event represents a data packet synchronized from Garmin Connect (e.g., 15 minutes of biometric data).

### 2.2 Environmental Events
- **Metric:** Total number of MQTT samples sent by the sensing units (Raspberry Pi).
- **Manual Query:**
  ```bash
  docker exec mongo mongo -u root -p example --authenticationDatabase admin thesis_db --eval "db.sensors_data_collection.count()"
  ```
- **Result:** **770** environmental events.
- **Description:** Each event corresponds to a reading of temperature, humidity, CO2, and TVOC sensors in one of the monitored rooms.

### 2.3 Presence Events
- **Metric:** Total number of state updates (ENTER/EXIT) produced via the presence CLI.
- **Manual Query (via Kafka offsets):**
  ```bash
  docker exec broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic user_presence
  ```
- **Result:** **59** presence events.
- **Description:** These events trigger the N-to-1 mapping logic, associating room sensors with the users present at that time.

### 2.4 Fused Samples
- **Metric:** Total number of rows generated after temporal fusion (at 1-minute resolution) between wearable and environmental data.
- **Manual Query:**
  ```bash
  docker exec mongo mongo -u root -p example --authenticationDatabase admin thesis_db --eval "db.merged_df_collection.count()"
  ```
- **Result:** **54,657** fused samples.
- **Distribution by Location (Query):**
  ```bash
  docker exec mongo mongo -u root -p example --authenticationDatabase admin thesis_db --eval "db.merged_df_collection.aggregate([{ \$group: { _id: '\$room', count: { \$sum: 1 } } }])"
  ```
- **Results:**
  - **Home:** **31.95%** (17,466 samples)
  - **Lab:** **64.69%** (35,359 samples)
  - **Library:** **3.35%** (1,832 samples)


## 3. System Performance Metrics

### 3.1 Forward-fill Rate
- **Metric:** Percentage of fused samples where environmental data was propagated forward due to the sampling frequency difference between wearables (1 min) and sensors (~60 min).
- **Formula:** `(Total Fused - Total Environmental) / Total Fused`
- **Result:** **98.6%**

### 3.2 Delayed Wearable Events (Backward Sync)
- **Metric:** Percentage of Garmin events that arrived with a significant delay (>1 hour) and were processed via retroactive synchronization with historical sensor data.
- **Manual Query:**
  ```bash
  docker exec mongo mongo -u root -p example --authenticationDatabase admin thesis_db --eval "
  var delayed = 0; var total = 0;
  db.wearable_data_collection.find().forEach(function(doc) {
      total++;
      if ((doc._id.getTimestamp() - new Date(doc.timestamp)) > 3600000) delayed++;
  });
  print('Delayed: ' + delayed + ' / ' + total + ' (' + (delayed/total*100).toFixed(2) + '%)');"
  ```
- **Result:** **13.45%**

### 3.3 Discarded Samples
- **Metric:** Percentage of initially fused samples discarded during the cleaning phase due to critical sensor gaps or out-of-range values.
- **Formula:** Comparison between `merged_df_collection` (54,657) and valid samples in training notebooks (~12,863).
- **Result:** **76.47%**


## Summary for Table 1 of the thesis doc
> The system collected **4,900** wearable events, **770** environmental events, **59** presence events, and **54,657** fused samples (home **31.95**%, lab **64.69**%, library **3.35**%). Forward-fill rate: **98.6**%; delayed wearable events processed via backward sync: **13.45**%; discarded samples: **76.47**%.
