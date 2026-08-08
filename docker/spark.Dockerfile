# ═══════════════════════════════════════════════════════════════════════════
# Spark Dockerfile
# ═══════════════════════════════════════════════════════════════════════════
# Based on official Apache Spark image with additional Python dependencies
# for the OLAP loading and analytics scripts.
#
# Compatible with both Docker and Podman.
# ═══════════════════════════════════════════════════════════════════════════

FROM docker.io/apache/spark:3.5.1

# Switch to root for package installation
USER root

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages compatible with Python 3.8 (image's default)
RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.16 \
    pyarrow==16.1.0 \
    pandas==2.0.3 \
    numpy==1.24.4

# Create data directories
RUN mkdir -p /opt/spark/data/raw \
             /opt/spark/data/lake \
             /opt/spark/data/staging \
             /opt/spark/scripts

# Switch back to spark user
USER spark

# Default working directory
WORKDIR /opt/spark
