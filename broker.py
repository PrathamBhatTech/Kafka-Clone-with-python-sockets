import socket
import sys
import threading

from time import sleep

# Kafka Broker
class Broker():
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.conn = None

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
        while True:
            sock.listen(1)
            print('Broker is listening on port ' + str(self.port) + '...')

            while True:
                self.conn, self.addr = sock.accept()
                print('Broker has accepted a connection from ' + str(self.addr))
                threading.Thread(target=self.multi_threaded_client, args=[self.conn,self.addr]).start()


    def multi_threaded_client(self, conn, addr):
        while True:
            data = conn.recv(2048)
            response = 'Server message: ' + data.decode('utf-8')
            if not data:
                break
            print('Broker has received a message from ' + str(addr) + ': ' + data.decode('utf-8'))
            conn.sendall(response.encode('utf-8'))
        conn.close()
        
    def zookeeper_heartbeat(self, conn):
        while True:
            conn.send('fBroker {self.host}:{self.port} is alive'.encode('utf-8'))
            ack = conn.recv(2048).decode('utf-8')
            sleep(3)


host, port = sys.argv[1].split(':')
Broker = Broker(host, int(port))