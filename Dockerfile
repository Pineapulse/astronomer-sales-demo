FROM astrocrpublic.azurecr.io/runtime:3.0-6

ENV AIRFLOW__WEBSERVER__CONNECTIONS__ALLOW_CONNECTION_TEST=True

# Switch to root to install packages
USER root

# Install any OS-level packages needed
RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to astro user
USER astro

# Copy requirements
COPY requirements.txt /usr/local/airflow/

# Install Python packages
RUN pip install --no-cache-dir -r /usr/local/airflow/requirements.txt


