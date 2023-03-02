
#!/bin/bash
python3 /root/opensource-gcp/main.py --source Mysql
export CONFLUENT_HOME=/root/confluent-7.2.2
export PATH=$PATH:$CONFLUENT_HOME/bin

confluent local services start

cd ..
cd  /root/confluent-7.2.2
cd ..
#echo "creating the topic"
#Command for opening kafka Consumer :
echo "Consumer is in running state"
confluent-7.2.2/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic Streaming-kafka  --from-beginning
