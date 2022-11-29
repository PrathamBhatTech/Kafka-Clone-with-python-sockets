from myKafka import KafkaProducer
import json
import sys

print('Kafka Producer is initializing...')
bootstrap_servers = ['localhost:9092']
topicname = sys.argv[1]
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
print('Kafka Producer has been initiated...')

while True:
    data = input()
    ack = producer.send(topicname, data.encode('utf-8'))