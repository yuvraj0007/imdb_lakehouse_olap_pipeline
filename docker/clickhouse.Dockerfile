# ═══════════════════════════════════════════════════════════════════════════
# ClickHouse Dockerfile
# ═══════════════════════════════════════════════════════════════════════════
# Based on official ClickHouse Alpine image.
# Includes custom configuration for local development.
#
# In most cases, the base image is sufficient and this Dockerfile
# is provided for extensibility (custom configs, UDFs, etc.)
# ═══════════════════════════════════════════════════════════════════════════

FROM clickhouse/clickhouse-server:24.3-alpine

# Copy custom user configuration
COPY clickhouse/config/users.xml /etc/clickhouse-server/users.d/users.xml

# Copy DDL to auto-execute on startup
COPY sql/ddl.sql /docker-entrypoint-initdb.d/001_ddl.sql

# Expose ports
EXPOSE 8123 9000

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD clickhouse-client --query "SELECT 1" || exit 1
