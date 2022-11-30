from myKafka import KafkaProducer
import json
import sys

print('Kafka Producer is initializing...')
bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
print('Kafka Producer has been initiated...')

while True:
    data = input()
    ack = producer.send(data.encode('utf-8'))