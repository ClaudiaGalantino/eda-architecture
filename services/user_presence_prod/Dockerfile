# Python image
FROM python:3.12-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy subscriber script
COPY users_presence.py .

# Force Python to flush print output immediately
ENV PYTHONUNBUFFERED=1

# Command to run the presence producer
CMD ["python", "-u", "users_presence.py"] 