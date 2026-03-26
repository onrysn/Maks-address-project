FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /etl

COPY requirements.txt /etl/requirements.txt
RUN pip install --no-cache-dir -r /etl/requirements.txt
