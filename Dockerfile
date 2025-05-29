# Dockerfile
FROM apache/airflow:2.9.1

# USER root

# Install system packages (if needed)
# RUN apt-get update && apt-get install -y curl

USER airflow

# Install Python packages
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
