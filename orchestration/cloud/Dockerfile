FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    JAVA_HOME=/opt/java-home \
    PIP_DEFAULT_TIMEOUT=300 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/opt/project

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        openjdk-17-jre-headless \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --retries 10 -r /tmp/requirements.txt

COPY . /opt/project

ENTRYPOINT ["python", "-m", "orchestration.cloud.task_runner"]
