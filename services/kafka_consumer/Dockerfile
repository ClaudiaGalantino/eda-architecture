# Use of lightweight Python image
FROM python:3.12-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements file first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy subscriber script
COPY kafka_consumer.py .

# Force Python to flush print output immediately
ENV PYTHONUNBUFFERED=1

# Run the producer automatically when the container starts?
# execute the producer when called
CMD ["python", "kafka_consumer.py"]