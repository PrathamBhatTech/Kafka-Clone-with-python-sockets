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
        print('Producer has connected to Zookeeper')
        
        self.conn = sock

        self.conn.send("Producer".encode('utf-8'))

        # Get broker host and port from zookeeper
        data = sock.recv(1024)
        print('Producer has received a message from Zookeeper: ' + data.decode('utf-8'))
        self.conn.close()
        print('Producer has disconnected from Zookeeper')

        # Connect to broker
        port = data.decode('utf-8')
        port = int(port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, port))
        print('Producer has connected to broker '+str(port))
        
        self.conn = sock

        self.conn.send("Producer".encode('utf-8'))

        print('Kafka Producer has been initiated...')

    def send(self, value=None):
        self.conn.send(str(value).encode('utf-8'))

        return self.conn.recv(1024)

# Kafka Consumer
class KafkaConsumer():
    def __init__(self, topicName, bootstrap_servers):
        self.topicName = topicName
        self.bootstrap_servers = bootstrap_servers

        # connect to zookeeper
        HOST, PORT = self.bootstrap_servers.split(':')
        PORT = int(PORT)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))

        self.conn = sock
        self.conn.send("Consumer".encode('utf-8'))
        self.conn.send(topicName.encode('utf-8'))

        # Get broker host and port from zookeeper
        data = sock.recv(1024)
        self.conn.close()
        port = data.decode('utf-8')
        port = int(port)
        
        # connect to broker
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, port))

        self.conn = sock

        self.conn.send("Consumer".encode('utf-8'))

        print('Kafka Consumer has been initiated...')

        data = self.conn.recv(1024)
        self.conn.send("ack".encode('utf-8'))

        print('Kafka Consumer has received a message from ' + str(self.bootstrap_servers) + ': ' + data.decode('utf-8'))

        if data.decode('utf-8') != 'yes':
            self.topic_exists = True
        else:
            self.topic_exists = False

    def topic_status(self):
        return self.topic_exists

    def __iter__(self):
        return self.consumer.__iter__()  # type: ignore

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.consumer.close()  # type: ignore

    def close(self):
        self.consumer.close()  # type: ignore

    def poll(self, timeout_ms=None, max_records=None, update_offsets=True):
<<<<<<< HEAD
        return self.consumer.poll(timeout_ms, max_records, update_offsets)  # type: ignore




=======
        return self.consumer.poll(timeout_ms, max_records, update_offsets)
>>>>>>> c10c58b08c562c91efc11ae21eaa3e4e014d8e32
