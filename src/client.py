import socket
import utils


# utils.send_msg('localhost',10,"123")

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 10))

sock.send("123".encode('utf-8'))
print(sock.recv(1024).decode('utf-8'))

# client1.close() #??????
# client2.close() #??????