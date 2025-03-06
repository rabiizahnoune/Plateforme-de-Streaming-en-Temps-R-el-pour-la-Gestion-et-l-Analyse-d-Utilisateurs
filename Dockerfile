# Utiliser une image de base Ubuntu
FROM ubuntu:22.04

# Mettre à jour les paquets et installer les dépendances de base
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    git \
    openjdk-11-jdk \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Variables d’environnement pour Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Installer Kafka (Confluent Community Edition)
RUN curl -O https://packages.confluent.io/archive/7.5/confluent-community-7.5.0.tar.gz \
    && tar -xzf confluent-community-7.5.0.tar.gz -C /opt/ \
    && ln -s /opt/confluent-7.5.0 /opt/confluent \
    && rm confluent-community-7.5.0.tar.gz
ENV PATH=$PATH:/opt/confluent/bin

# Installer Spark (avec PySpark)
RUN wget -O spark-3.5.0-bin-hadoop3.tgz "https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz" \
    && tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /usr/local/ \
    && ln -s /usr/local/spark-3.5.0-bin-hadoop3 /usr/local/spark \
    && rm spark-3.5.0-bin-hadoop3.tgz
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:/usr/lib/python3/dist-packages

# Installer MongoDB (serveur communautaire)
RUN wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add - \
    && echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu focal/multiverse amd64" > /etc/apt/sources.list.d/mongodb-org-6.0.list \
    && apt-get update \
    && apt-get install -y mongodb-org \
    && mkdir -p /data/db \
    && rm -rf /var/lib/apt/lists/*

# Installer Cassandra
RUN echo "deb https://debian.cassandra.apache.org 41x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list \
    && curl -L https://debian.cassandra.apache.org/41x/key | apt-key add - \
    && apt-get update \
    && apt-get install -y cassandra \
    && rm -rf /var/lib/apt/lists/*

# Installer les dépendances Python
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copier le code du projet
COPY . /app

# Script pour démarrer tous les services
COPY start-services.sh /app/start-services.sh
RUN chmod +x /app/start-services.sh

# Exposer les ports nécessaires
EXPOSE 9092 27017 9042 8501

# Commande par défaut
CMD ["/app/start-services.sh"]