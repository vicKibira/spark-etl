FROM apache/spark:4.0.0-python3

USER root

# GCS connector so gs:// paths work out of the box
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/gcs-connector-hadoop3-latest.jar

# Python deps for your jobs
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Default Spark + GCS config for every container
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN mkdir -p /opt/apps /opt/data /opt/keys && chown -R spark:spark /opt/apps /opt/data

USER spark
WORKDIR /opt/apps
