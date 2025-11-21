# Python image
FROM python:3.12-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy subscriber script
COPY wearable_producer.py .

# Force Python to flush print output immediately
ENV PYTHONUNBUFFERED=1

# Run the producer automatically when the container starts
CMD ["python", "wearable_producer.py"]