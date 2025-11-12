FROM python:3.11-slim

WORKDIR /usr/app

# Install git (optional but recommended for dbt packages)
RUN apt-get update && apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Install dbt-core and dbt-snowflake
RUN pip install --no-cache-dir dbt-core dbt-snowflake

# Set dbt profiles directory
ENV DBT_PROFILES_DIR=/root/.dbt

# Default command
ENTRYPOINT ["dbt"]

