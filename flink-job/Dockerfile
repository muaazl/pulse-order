FROM flink:1.19.1-scala_2.12-java11

# Install Python 3, pip, PyFlink, and Redis client
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-dev curl && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt

# Download official Flink Kafka SQL/DataStream connector JAR
RUN curl -fsSL https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar \
    -o /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar

COPY surge_window_job.py /opt/flink/usrlib/surge_window_job.py
