# Use Python base image
FROM python:3.9-slim

# Install system dependencies
RUN apt-get update \
    && apt-get install -y unzip libaio1 wget \
    && rm -rf /var/lib/apt/lists/*

# Install Oracle Instant Client
RUN mkdir -p /opt/oracle \
    && cd /opt/oracle \
    && wget https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basic-linux.x64-21.1.0.0.0.zip \
    && wget https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-sdk-linux.x64-21.1.0.0.0.zip \
    && wget https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-sqlplus-linux.x64-21.1.0.0.0.zip \
    && unzip instantclient-basic-linux.x64-21.1.0.0.0.zip \
    && unzip instantclient-sdk-linux.x64-21.1.0.0.0.zip \
    && unzip instantclient-sqlplus-linux.x64-21.1.0.0.0.zip \
    && rm instantclient-basic-linux.x64-21.1.0.0.0.zip \
    && rm instantclient-sdk-linux.x64-21.1.0.0.0.zip \
    && rm instantclient-sqlplus-linux.x64-21.1.0.0.0.zip \
    && echo /opt/oracle/instantclient_21_1 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

# Set environment variables for Oracle Instant Client
ENV ORACLE_HOME=/opt/oracle/instantclient_21_1
ENV LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH

# Install Python dependencies
COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY . /app
WORKDIR /app

# Command to run the script
CMD ["python", "welsh-trust-greylit.py"]
