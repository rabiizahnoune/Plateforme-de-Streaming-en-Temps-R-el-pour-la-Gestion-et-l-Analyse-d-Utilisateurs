#demarer un contenaire ubunto et ouvrir les ports necessaire et creer workspace app pour mes scripts
docker run -it --name streaming-container -p 9092:9092 -p 27017:27017 -p 9042:9042 -p 8501:8501 -v $(pwd):/app ubuntu:22.04

ps aux | grep ingest_data.py 

docker pull ubuntu

docker run -itd -p 8080:8080 --name spark --hostname ubuntu


apt update

apt -y upgrade
#install curl wget git python python-pip java-11
apt-get update && apt-get install -y curl wget git python3 python3-pip openjdk-11-jdk


#install kafka
curl -O https://packages.confluent.io/archive/7.5/confluent-community-7.5.0.tar.gz
tar -xzf confluent-community-7.5.0.tar.gz -C /opt/
ln -s /opt/confluent-7.5.0 /opt/confluent
rm confluent-community-7.5.0.tar.gz


#demarer Zookkeeper
zookeeper-server-start /opt/confluent/etc/kafka/zookeeper.properties &

#demarer kafka
kafka-server-start /opt/confluent/etc/kafka/server.properties &

#creer un topic 
kafka-topics --create --topic user-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1


#telecharger spark
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /usr/local/
ln -s /usr/local/spark-3.5.0-bin-hadoop3 /usr/local/spark
rm spark-3.5.0-bin-hadoop3.tgz


# Configurer les variables d’environnement 
echo 'export SPARK_HOME=/usr/local/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
echo 'export PYTHONPATH=$SPARK_HOME/python:/usr/lib/python3/dist-packages' >> ~/.bashrc
source ~/.bashrc


#configurer mongodb
apt tee /etc/yum.repos.d/mongodb-org-6.0.repo <<EOF
[mongodb-org-6.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/\$releasever/mongodb-org/6.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-6.0.asc
EOF




#demarer mongo db
systemctl start mongod
systemctl enable mongod






























































apt install default-jdk

java -version


apt install scala

apt install wget
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

tar -xzf spark-3.5.0-bin-hadoop3.tgz

mv spark-3.5.0-bin-hadoop3 /opt/spark


echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.bashrc
source ~/.bashrc



start-master.sh

start-slave.sh spark://spark:7077




###################################installation d un cluster 
docker exec -it spark bash
apt install openssh-server openssh-client
ssh-keygen -t rsa -P ""
cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
vim ~/.bashrc
source ~/.bashrc
ls $$SPARK_HOME/conf
ls $SPARK_HOME/conf
ls
cd var
ls
cd ..
cd ..
pwd
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh

pour copier une image d un contenaire 
docker commit spark spark-image