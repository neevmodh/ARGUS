# Multi-arch Hadoop image (the official apache/hadoop image is amd64-only).
# Hadoop is pure Java, so the generic tarball runs natively on arm64 JVMs too.
FROM eclipse-temurin:11-jre-jammy

ARG HADOOP_VERSION=3.4.1
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl procps netcat-openbsd python3 \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-lean.tar.gz" \
    | tar -xz -C /opt \
 && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm -rf /opt/hadoop/share/doc

COPY config/ ${HADOOP_CONF_DIR}/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/yarn

WORKDIR /opt/hadoop
ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
