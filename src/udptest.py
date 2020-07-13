import socket
import json, os
f1 = open('test.py', mode='rb')
content = f1.read()
f1.close()
file = ["123.py", content.decode("utf-8"), './new/']

reply = "download：" + json.dumps(file)
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(reply.encode("utf-8"), ('127.0.0.1', 10))

# os.makedirs("./asd:asd")