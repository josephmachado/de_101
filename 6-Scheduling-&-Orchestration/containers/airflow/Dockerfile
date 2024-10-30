FROM apache/airflow:2.9.2

# Install requirements
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

User root

# RUN python $AIRFLOW_HOME/setup_conn.py
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#     default-jdk

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk
