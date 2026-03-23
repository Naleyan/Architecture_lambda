FROM spark:4.0.1-java21-python3

USER root

# Install only necessary Python libs
RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    lz4

# Optionnel (debug réseau)
RUN apt-get update && apt-get install -y netcat

# Variables Spark
ENV SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

# Dossiers utiles
WORKDIR /opt/spark-apps

RUN mkdir -p /opt/spark/logs

# Script de démarrage
COPY start-spark.sh /
RUN chmod +x /start-spark.sh

CMD ["/bin/bash", "/start-spark.sh"]