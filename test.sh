zookeeper-server-start /opt/confluent/etc/kafka/zookeeper.properties > /var/log/zookeeper.log 2>&1 &
sleep 5


kafka-server-start /opt/confluent/etc/kafka/server.properties > /var/log/kafka.log 2>&1 &
sleep 5



mongod --fork --logpath /var/log/mongodb.log