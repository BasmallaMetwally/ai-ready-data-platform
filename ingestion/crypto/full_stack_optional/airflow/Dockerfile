FROM apache/airflow:2.9.3-python3.11

USER root
# Spark's submit client needs a JVM; procps is required by spark-submit's
# process handling inside the container.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/airflow/src:/opt/airflow/dq
ENV DBT_PROFILES_DIR=/opt/airflow/dbt
