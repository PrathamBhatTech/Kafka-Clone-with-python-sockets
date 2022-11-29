import socket
import threading

class Zookeeper():
    def __init__(self):
        self.host = "localhost"
        self.port = 9092
        self.conn = None

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
            # TODO: if broker doesn't send heartbeat for 10 seconds, remove it from the list
            data = conn.recv(2048)
            if not data:
                break
            print('mini Zookeeper has received a message from ' + str(addr) + ': ' + data.decode('utf-8'))
            conn.sendall("ack".encode('utf-8'))

        conn.close()