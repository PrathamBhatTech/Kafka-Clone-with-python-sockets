import json
from myKafka import KafkaConsumer

try:
    consumer = KafkaConsumer(topicName = 'test', bootstrap_servers = 'localhost:9092')
    topicStatus = consumer.topic_status()
    while True and topicStatus:
        topicLocation = str("./topic/"+topicName+".json")

        if os.path.isfile(topicLocation):
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