import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# sock.connect((guid_ip, guid_port))
sock.sendto("fasong".encode('utf-8'), ("127.0.0.1", 10))