import os.path
import json
from myKafka import KafkaConsumer

while True:
    topicName = input("\nEnter topicName consumer subscribes to :")
    oldData = []

    try:
        while True:
            ## - send this topic to broker for every iteration

            # Broker sends yes/no to consumer.
            ack = [True,False] #recieve this value

            topicLocation = str("./topic/"+topicName+".json")

            if os.path.isfile(topicLocation) == True and ack == True:
                f = open('data.json')
                temp = json.load(f)
                data = temp["val"]
                for topic in list(set(temp) - set(oldData)):
                    print(topic)        

            else:
                # print("No new data for :",topicName)
                pass

    except KeyboardInterrupt:
        print('Unsubscribed!')