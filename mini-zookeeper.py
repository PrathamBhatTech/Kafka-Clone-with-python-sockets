import socket
import threading

import asyncio

class Zookeeper():
    def __init__(self):
        self.HOST = "localhost"
        self.PORT = 9092
        self.conn = None

        host = "localhost"

        alive_brokers = []

        # Listen for broker connections
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.HOST, self.PORT))
        sock.settimeout(10)
        sock.listen(1)

        while True:
            self.conn, self.addr = sock.accept()
            print('Zookeeper has accepted a connection from ' + str(self.addr))
            data = self.conn.recv(2048)
            if data.decode('utf-8').split(' ')[0] != 'Broker':
                port = alive_brokers[0]
                self.conn.send(str(port).encode('utf-8'))
                self.conn.close()
            else:
                alive_brokers.append(data.decode('utf-8').split(' ')[1])
                threading.Thread(target=self.multi_threaded_broker, args=[self.conn,self.addr]).start()

    def multi_threaded_broker(self, conn, addr):
        while True:
            # TODO: if broker doesn't send heartbeat for 10 seconds, remove it from the list
            # asyncio.run(self.no_time_to_die())
            data = conn.recv(2048)
            if not data:
                alive_brokers.pop_left()
                break
            print('mini Zookeeper has received a message from ' + str(addr) + ': ' + data.decode('utf-8'))
            conn.sendall("ack".encode('utf-8'))

        conn.close()

    async def no_time_to_die(self):
        self.conn.close()

Zookeeper()