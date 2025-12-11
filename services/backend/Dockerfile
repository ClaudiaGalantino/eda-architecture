# Python image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get purge -y gcc librdkafka-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the rest of the source code into the image
COPY . .

# Expose the port on which Gunicorn will run
EXPOSE 5000

# Command to start the application with Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "wsgi:app"]