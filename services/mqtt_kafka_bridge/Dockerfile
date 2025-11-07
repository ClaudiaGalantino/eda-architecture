# Use of lightweight Python image
FROM python:3.12-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements file first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy subscriber script
COPY mqtt_kafka_bridge.py .

# Force Python to flush print output immediately
ENV PYTHONUNBUFFERED=1

# Run the subscriber automatically when the container starts
CMD ["python", "mqtt_kafka_bridge.py"]
