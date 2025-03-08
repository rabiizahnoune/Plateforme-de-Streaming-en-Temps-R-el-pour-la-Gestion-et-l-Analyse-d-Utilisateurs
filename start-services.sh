#!/bin/bash

# Démarrer ZooKeeper (requis par Kafka)
zookeeper-server-start /opt/confluent/etc/kafka/zookeeper.properties > /dev/null 2>&1 &

# Attendre un peu pour que ZooKeeper soit prêt
sleep 5

# Démarrer Kafka
kafka-server-start /opt/confluent/etc/kafka/server.properties > /dev/null 2>&1 &

# Démarrer MongoDB
mongod --fork --logpath /var/log/mongodb.log
mongosh --host localhost --port 27017

# Démarrer Cassandra
cassandra -f > /dev/null 2>&1 &

# Attendre que tous les services soient opérationnels
sleep 10

# Créer un topic Kafka (si non existant)
kafka-topics --create --topic user-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 > /dev/null 2>&1 || true

# Garder le conteneur actif avec un terminal interactif
exec bash