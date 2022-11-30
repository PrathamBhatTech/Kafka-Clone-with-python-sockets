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
    except IOError as e:
        print('Broker is down. Reconnecting...')
        producer.reconnectToBroker()
    except Exception as e:
        print(e)
        if e == "[Errno 104] Connection reset by peer" or e == "[Errno 32] Broken pipe":
            producer.reconnectToBroker()