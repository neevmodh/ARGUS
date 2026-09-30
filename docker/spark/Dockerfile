FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu

USER root

# Kafka source for Structured Streaming, baked in so jobs start without --packages downloads.
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar \
 && curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.6/spark-token-provider-kafka-0-10_2.12-3.5.6.jar \
 && curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY argus-submit /usr/local/bin/argus-submit
RUN chmod +x /usr/local/bin/argus-submit && mkdir -p /opt/spark/work /tmp/spark-events && chown -R spark /opt/spark/work /tmp/spark-events

ENV PYTHONPATH=/opt/argus/src \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH

USER spark
WORKDIR /opt/argus
