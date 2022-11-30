from myKafka import KafkaProducer
import json
import sys

print('Kafka Producer is initializing...')
bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
print('Kafka Producer has been initiated...')

while True:
    data = input()
    try:
        ack = producer.send(data)
    except Exception as e:
        producer.reconnectToBroker()