import json
from myKafka import KafkaConsumer
import os.path


topicName = input("\nEnter topicName consumer subscribes to :")
oldData = []
consumer = KafkaConsumer(topicName , bootstrap_servers = 'localhost:9092')
topicStatus = consumer.topic_status()
while topicStatus:
    try:
        topicLocation = str("./topic/"+topicName+".json")
        f = open('data.json')
        temp = json.load(f)
        data = temp["val"]
        for newTopic in list(set(temp) - set(oldData)):
            print(newTopic)        
        else:
            # print("No new data for :",topicName)
            pass

    except KeyboardInterrupt:
        print('Unsubscribed!')
        break
