# Python image
FROM python:3.12-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements file
COPY requirement.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirement.txt

# Copy subscriber script
COPY edge_initiation.py .

# Force Python to flush print output immediately
ENV PYTHONUNBUFFERED=1

# Run the edge initiation script
CMD ["python", "-u", "edge_initiation.py"]