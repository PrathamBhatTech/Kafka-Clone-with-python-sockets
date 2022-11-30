# simulate the kafka library

import socket


# Kafka Producer
class KafkaProducer():
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers

        self.connectToZookeeper()
        self.connectToBroker()
        
        print('Kafka Producer has been initiated...')

    def connectToZookeeper(self):
        # connect to zookeeper
        self.HOST, PORT = self.bootstrap_servers[0].split(':')
        PORT = int(PORT)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.HOST, PORT))
        print('Producer has connected to Zookeeper')
        
        self.conn = sock

        self.conn.send("Producer".encode('utf-8'))
        self.broker_port = sock.recv(1024).decode('utf-8')
        print('Producer has received a message from Zookeeper: ' + self.broker_port)
        self.conn.close()
        print('Producer has disconnected from Zookeeper')

    def connectToBroker(self):
        # Connect to broker
        port = self.broker_port
        port = int(port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.HOST, port))
        print('Producer has connected to broker '+str(port))

        self.conn = sock

        self.conn.send("Producer".encode('utf-8'))

    def reconnectToBroker(self):
        self.connectToZookeeper()
        self.connectToBroker()

    def send(self, value=None):
        self.conn.send(str(value).encode('utf-8'))

        return self.conn.recv(1024)

# Kafka Consumer
class KafkaConsumer():
    def __init__(self, topicName, bootstrap_servers):
        self.topicName = topicName
        self.bootstrap_servers = bootstrap_servers
        print('Kafka Consumer has been initiated...')

        self.connectToZookeeper()
        self.connectToBroker()
        

    def connectToZookeeper(self):
        # connect to zookeeper
        self.HOST, PORT = self.bootstrap_servers.split(':')
        PORT = int(PORT)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.HOST, PORT))
        print('Consumer has connected to Zookeeper')
        
        self.conn = sock

        self.conn.send("Consumer".encode('utf-8'))
        self.broker_port = sock.recv(1024).decode('utf-8')
        print('Consumer has received a message from Zookeeper: ' + self.broker_port)
        self.conn.close()

        print('Consumer has disconnected from Zookeeper')

    def connectToBroker(self):
        # Connect to broker
        port = self.broker_port
        port = int(port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((self.HOST, port))
        print('Consumer has connected to broker '+str(port))

        self.conn = sock

        self.conn.send("Consumer".encode('utf-8'))

        data = self.conn.recv(1024)
        self.conn.send("ack".encode('utf-8'))

        print('Kafka Consumer has received a message from ' + str(self.bootstrap_servers) + ': ' + data.decode('utf-8'))

        if data.decode('utf-8') != 'yes':
            self.topic_exists = True
        else:
            self.topic_exists = False

    def topic_status(self):
        return self.topic_exists

    def checkBroker(self):
        # if response is not received, reconnect to broker
        try:
            self.conn.send("check".encode('utf-8'))
            if self.conn.recv(1024).decode('utf-8') != 'ack':
                self.reconnectToBroker()
        except:
            self.reconnectToBroker()

    def reconnectToBroker(self):
        self.connectToZookeeper()
        self.connectToBroker()

    def close(self):
        self.consumer.close()

    def poll(self, timeout_ms=None, max_records=None, update_offsets=True):
        return self.consumer.poll(timeout_ms, max_records, update_offsets)
