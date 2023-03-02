
#!/bin/bash
python3 /root/Orcale-schema-migration/main.py --source Mysql
export CONFLUENT_HOME=/root/confluent-7.2.2
export PATH=$PATH:$CONFLUENT_HOME/bin

confluent local services start

cd ..
cd  /root/confluent-7.2.2


#If You want to create a new Topic :
#topic = demolive

#python3 mysql-python.py
cd ..
#echo "creating the topic"

confluent-7.2.2/bin/kafka-topics --create --topic Streaming-kafka --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

#Command for opening kafka Consumer :
echo "Consumer is in running state"
confluent-7.2.2/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic Streaming-kafka  --from-beginning