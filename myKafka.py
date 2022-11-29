# simulate the kafka library

import socket


# Kafka Producer
class KafkaProducer():
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers

        # connect to zookeeper
        HOST, PORT = self.bootstrap_servers[0].split(':')
        PORT = int(PORT)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        
        self.conn = sock

        self.conn.send("Producer".encode('utf-8'))

        # Get broker host and port from zookeeper
        data = sock.recv(1024)
        self.conn.close()

        # Connect to broker
        host, port = data.decode('utf-8').split(':')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        
        self.conn = sock

        self.conn.send("Producer".encode('utf-8'))

        print('Kafka Producer has been initiated...')

    def send(self, topic, value=None):
        self.conn.send(f'{topic}:{value}'.encode('utf-8'))

        return self.conn.recv(1024)

# Kafka Consumer
class KafkaConsumer():
    def __init__(self, topicName, bootstrap_servers, auto_offset_reset, value_deserializer):
        self.topicName = topicName
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset
        self.value_deserializer = value_deserializer

        # connect to zookeeper
        HOST, PORT = self.bootstrap_servers[0].split(':')
        PORT = int(PORT)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))

        self.conn = sock
        self.conn.send("Consumer".encode('utf-8'))
        self.conn.send(topicName.encode('utf-8'))

        # Get broker host and port from zookeeper
        data = sock.recv(1024)
        self.conn.close()
        host, port = data.decode('utf-8').split(':')
        
        # connect to broker
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, int(port)))

        self.conn = sock

        self.conn.send("Consumer".encode('utf-8'))

        print('Kafka Consumer has been initiated...')

        while True:
            data = self.conn.recv(1024)
            
            if not data:
                break

            print('Kafka Consumer has received a message from ' + str(self.bootstrap_servers) + ': ' + data.decode('utf-8'))

            if data.decode('utf-8') != '':
                self.conn.send("ack".encode('utf-8'))

    def __iter__(self):
        return self.consumer.__iter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.consumer.close()

    def close(self):
        self.consumer.close()

    def poll(self, timeout_ms=None, max_records=None, update_offsets=True):
        return self.consumer.poll(timeout_ms, max_records, update_offsets)




