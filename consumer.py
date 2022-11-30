import json
from myKafka import KafkaConsumer
import os.path
import csv

topicName = input("\nEnter topicName consumer subscribes to :")
oldData = []
consumer = KafkaConsumer(topicName , bootstrap_servers = 'localhost:9092')
topicStatus = consumer.topic_status()
print(topicStatus)
while True:
    try:
        consumer.checkBroker()
        topicLocation = str("topics/"+topicName+".csv")
        if os.path.isfile(topicLocation) == True:
            temp = []
            f = open(topicLocation, "r")
            reader = csv.reader(f)
            for row in reader:
                temp.append(row)
            f.close()

            for newTopic in temp[len(oldData):]:
                print(newTopic)
                oldData = temp
        else:
            print("Topic does not exist")
            pass

    except KeyboardInterrupt:
        consumer.close()
        consumer.reconnectToBroker()
        print('Unsubscribed!')
        break