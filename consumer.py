import json
from myKafka import KafkaConsumer
import os.path
import csv
import sys

argument=None
if len(sys.argv) > 1:
    argument = sys.argv[1]
    

topicName = input("\nEnter topicName consumer subscribes to :")
topicLocation = str("topics/"+topicName+".csv")

oldData = []

if argument != '--from-beginning':
    if os.path.isfile(topicLocation) == True:
        temp = []
        f = open(topicLocation, "r")
        reader = csv.reader(f)
        for row in reader:
            temp.append(row)
        f.close()

        oldData = temp

consumer = KafkaConsumer(topicName , bootstrap_servers = 'localhost:9092')
topicStatus = consumer.topic_status()
print(topicStatus)
while True:
    try:
        consumer.checkBroker()
        
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