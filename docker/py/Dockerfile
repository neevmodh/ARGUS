# Lightweight image for the Kafka producer, Streamlit dashboard and helper scripts.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/opt/argus/src
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/argus
