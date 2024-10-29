# Use a lightweight Python base image
FROM python:3.10-slim

# Set environment variables
ENV DBT_VERSION="1.8.0"

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-core
RUN pip install dbt-core==$DBT_VERSION

# Optionally install other dbt adapters, like dbt-postgres, dbt-snowflake, etc.
RUN pip install dbt-duckdb==$DBT_VERSION

# Set up a working directory
WORKDIR /dbt

# Add your dbt project to the Docker image (optional if you want to add dbt project files)
# COPY . .

# Set default command to run dbt --help
CMD ["dbt", "--help"]
