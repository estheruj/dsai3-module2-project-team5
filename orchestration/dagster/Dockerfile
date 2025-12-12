FROM python:3.10-slim

ENV DAGSTER_HOME=/opt/dagster \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/opt/dagster/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster

COPY orchestration/dagster/requirements.txt /opt/dagster/requirements.txt
RUN pip install --no-cache-dir -r /opt/dagster/requirements.txt

# Dagster code (repositories, jobs, schedules)
COPY orchestration/dagster /opt/dagster/app

# Ports: Dagster webserver
EXPOSE 3000

# Command provided by docker-compose per service (webserver/daemon)


