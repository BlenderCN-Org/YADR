import socket
import sys

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.connect(('localhost', 8888))
request = None

try:
    while request != 'quit':
        request = input('>> ')
        if request:
            server.send(request.encode('utf8'))
            response = server.recv(255).decode('utf8')
            print(response)
except KeyboardInterrupt:
    server.close()
