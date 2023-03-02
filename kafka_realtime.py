mport datetime
from google.cloud.bigquery import Table
import datetime
from kafka import KafkaProducer
import json
from kafka import KafkaConsumer
import sys
from google.cloud import bigquery
import os
import time
import mysql.connector
from google.cloud.exceptions import NotFound

import datetime
import pandas as pd

# ct stores current time
BQ_max_row = 0
result_bqmaxts = datetime.datetime.now()

# print("current time:-", ct)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/searce-practice-data-analytics-0b5e68683a7a.json"
project_nm = 'searce-practice-data-analytics'
table_id='searce-practice-data-analytics.gcp_roadshow_demo.product'
client = bigquery.Client(project_nm)
#dataset = client.dataset(dataset_nm)
table_ref = 'searce-practice-data-analytics.gcp_roadshow_demo.product'
bootstrap_servers = ['localhost:9092']
topicName = 'Streaming-kafka'
max_row_id_bq = 0
def create_table():
    client.get_table(table_id)  # Make an API request.
    print("Table {} already exists.".format(table_id))
    while (True):
        streaming()
def streaming():
    client.get_table(table_ref)
    #        sql = " SELECT max(EXTRACT(DATETIME FROM {})) FROM {} ".format("TimeStamp",table_id)
    sql = " select count(*) from `{}` LIMIT 1; ".format(table_id)

    job = client.query(sql)
#    print(job, "------------------------------------job--------------------------")
    row_bq = pd.DataFrame(job)
 #   print(row_bq, "------------------------------------------row_bq---------------------------")
    isempty = row_bq.empty
  #  print(isempty)
    if isempty == 'True':
        db = mysql.connector.connect(host='127.0.0.1', user='root', passwd='rootuser#123', db='gcp_roadshow_demo',
                                     port=3306)
        cursor = db.cursor(dictionary=True)

        query = "select * from product where Row_Id == 1"
   #     print(query, "------------------------ if --------------------------------")
        cursor.execute(query);
        data = cursor.fetchall()

        for item in data:
            send_msg = json.dumps(item, indent=4, sort_keys=True, default=str).encode('utf-8')
            print("send_msg: ", send_msg)
            print("send_msg_type:", type(send_msg))

            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            ack = producer.send(topicName, send_msg)
            metadata = ack.get()
            print("topic", metadata.topic)
            print("Partition", metadata.partition)
            print("Message Sent")

            consumer = KafkaConsumer(topicName, group_id='group1', bootstrap_servers=bootstrap_servers,
                                     auto_offset_reset='earliest', consumer_timeout_ms=30000)
            print("success")
            print("Consumer", consumer)
        for msg in consumer:
            print("Checking  the consumer for  messeages")
            temp = msg.value
            output = temp.decode()
            json_object = json.loads(output)
            print("jsontype:", json_object)
            row_to_insert = [json_object]
            staging_table_id = Table.from_string("searce-practice-data-analytics.gcp_roadshow_demo.product")
            errors = client.insert_rows_json(staging_table_id, row_to_insert)
        if (errors == []):
            print("Messeage inserted")
        else:
            print("Failed: ", errors)
            print("Iteration completed")
            print("starting the new")
    else:
        for row in row_bq.index:
            max_row_id_bq = row_bq[0][row]
        #    print(max_row_id_bq, "-----------------------------------------------------------------------------")
        max_row_id_bq = str(max_row_id_bq)
        max_row_id_bq = max_row_id_bq.replace("Row((", " ").replace(",), {'f0_': 0})", " ")
        max_row_id_bq = int(max_row_id_bq)
        #   print(" max_row_id_bq", max_row_id_bq)
        db = mysql.connector.connect(host='127.0.0.1',
                                     user='root',
                                     passwd='rootuser#123',
                                     db='gcp_roadshow_demo',
                                     port=3306)
        cursor = db.cursor(dictionary=True)
        TABLE = "product"
        query = "select Row_Id from {} ORDER BY Row_Id DESC LIMIT 1;".format(TABLE)
        #  print(query)
        cursor.execute(query);
        db_max = cursor.fetchall()
        # print(db_max, "-------------row max db-------------")
        query = "select * from product where Row_Id > {} ;".format(max_row_id_bq)
        # print(query)
        cursor.execute(query);
        data = cursor.fetchall()

        for item in data:
            send_msg = json.dumps(item, indent=4, sort_keys=True, default=str).encode('utf-8')
            print("send_msg: ", send_msg)
            print("send_msg_type:", type(send_msg))

            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            ack = producer.send(topicName, send_msg)
            metadata = ack.get()
            print("topic", metadata.topic)
            print("Partition", metadata.partition)
            print("Message Sent")
            consumer = KafkaConsumer(topicName, group_id='group1', bootstrap_servers=bootstrap_servers,
                                     auto_offset_reset='earliest', consumer_timeout_ms=30000)
            print("success")
            print("Consumer", consumer)
            for msg in consumer:
                print("Checking  the consumer for  messeages")
                temp = msg.value
                output = temp.decode()
                json_object = json.loads(output)
                print("jsontype:", json_object)
                row_to_insert = [json_object]
                staging_table_id = Table.from_string("searce-practice-data-analytics.gcp_roadshow_demo.product")
                errors = client.insert_rows_json(staging_table_id, row_to_insert)
                if errors == []:
                    print("Messeage inserted")
                else:
                    print("Failed: ", errors)
                    print("Iteration completed")
                    print("starting the new iteration")


create_table()

