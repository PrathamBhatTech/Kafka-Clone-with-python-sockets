import socket
import sys
import threading
import json
import os.path
from time import sleep

# Kafka Broker
class Broker():
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.conn = None

        self.producers = []
        self.consumers = {}

        # Host and port of the Zookeeper
        HOST, PORT = "localhost", 9092

        # Connect to Zookeeper
        sock_zookeeper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_zookeeper.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_zookeeper.connect((HOST, PORT))
        self.conn = sock_zookeeper
        print('Broker has connected to Zookeeper')

        # Start heartbeat thread
        threading.Thread(target=self.zookeeper_heartbeat, args=(self.conn,)).start()


        # Listen for connections
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(1)
        print('Broker is listening on port ' + str(self.port) + '...')

        while True:
            self.conn, self.addr = sock.accept()
            print('Broker has accepted a connection from ' + str(self.addr))

            prodcons = self.conn.recv(2048).decode('utf-8')

            if prodcons == 'Producer':
                self.producers.append(self.addr)
                threading.Thread(target=self.multi_threaded_publisher, args=(self.conn,self.addr)).start()
            else:
                topic = self.conn.recv(2048).decode('utf-8')
                if topic in self.consumers:
                    self.consumers[topic].append(self.conn)
                else:
                    self.consumers[topic] = self.conn

        
    def multi_threaded_publisher(self, conn, addr):
        while True:
            data = conn.recv(2048)
            conn.sendall("ack".encode('utf-8'))

            if not data:
                continue

            else:
                topic, value = data.decode('utf-8').split(':', 1)
                print('Broker has received a message from ' + str(addr) + ': ' + data.decode('utf-8'))

                # store this new data into directory of file
                topicLocation = str('./topic/'+topic+'.json')

                # topic already exists
                if os.path.isfile(topicLocation) == True:
                    
                    with open(topicLocation, "r") as jsonFile:
                        existingData = json.load(jsonFile)
                    
                    updatedData = {"val":existingData["val"].append(value)} 

                    with open(topicLocation, "w") as jsonFile:
                        json.dump(updatedData, jsonFile)

                # topic is new
                else:
                    newData = {"val":[value]} 
                    with open(topicLocation, "w") as jsonFile:
                        json.dump(newData, jsonFile)

                # NEEDS CHANGE
                # hb
                if topic in self.consumers:
                    for consumer in self.consumers[topic]:
                        consumer.sendall(value)


    def zookeeper_heartbeat(self, conn):
        conn.send('Broker'.encode('utf-8'))
        while True:
            conn.send(f'Broker {self.host}:{self.port} is alive'.encode('utf-8'))
            ack = conn.recv(2048).decode('utf-8')
            sleep(3)


host, port = sys.argv[1].split(':')
Broker = Broker(host, int(port))  # type: ignore