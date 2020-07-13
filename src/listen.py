import socketserver
import json
import time
from utils import *

class Listen_Server(socketserver.UDPServer):
    def __init__(self, server_address, RequestHandlerClass, node):
        self.node = node
        socketserver.UDPServer.__init__(self, server_address, RequestHandlerClass)

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request[0].decode("utf-8")
        socket = self.request[1]
        # print("\n"+data)

        data_array = data.split("：")
        if (data_array[1] != ''):
            try:
                info = json.loads(data_array[1])
            except:
                pass

        if data_array[0] == "join":
            targe = (info[1],info[2])
            id = info[0]

            if (self.server.node.next[0] != self.server.node.id):
                if(self.server.node.check_min()):
                    if (id < self.server.node.id):
                        self.server.node.pred = info
                        reply = "you_next：" + json.dumps(self.server.node.info)
                        socket.sendto(reply.encode('utf-8'), targe)
                        pass

                if (id > self.server.node.id and id < self.server.node.next[0]):
                    self.server.node.next = info
                    reply = "you_next：" + json.dumps(self.server.node.next)
                    socket.sendto(reply.encode('utf-8'), targe)
                elif(id < self.server.node.id and id > self.server.node.pred[0]):
                    self.server.node.pred = info
                    reply = "you_next：" + json.dumps(self.server.node.info)
                    socket.sendto(reply.encode('utf-8'), targe)

                else:

                    table = self.server.node.find_successor(info[0])[:]
                    if (table[0] == self.server.node.id):
                        reply = "ask：" + json.dumps(self.server.node.find_successor(id))
                        socket.sendto(reply.encode('utf-8'), targe)
                    else:
                        reply = "ask：" + json.dumps(self.server.node.next)
                        socket.sendto(reply.encode('utf-8'), (info[1], info[2]))
            else:
                self.server.node.next = info
                self.server.node.pred = info
                self.server.node.update_finger()
                reply = "you_next：" + json.dumps(self.server.node.info)
                socket.sendto(reply.encode('utf-8'), targe)
                reply = "you_pred：" + json.dumps(self.server.node.info)
                socket.sendto(reply.encode('utf-8'), targe)
                socket.sendto("update_finger：".encode('utf-8'), targe)

        if data_array[0] == "update_finger" :
            self.server.node.update_finger()

        if data_array[0] == "find_successor" :
            if (len(info) == 4):
                targe = (info[2], info[3])
                table = self.server.node.find_successor(info[1])[:]
                table.insert(0,info[0])
                reply = "you_finger：" + json.dumps(table)
                socket.sendto(reply.encode('utf-8'), targe)
            else:
                targe = (info[1], info[2])
                table = self.server.node.find_successor(info[0])[:]
                if (table[0] == self.server.node.id):
                    reply = "you_pred：" + json.dumps(self.server.node.info)
                    socket.sendto(reply.encode('utf-8'), targe)
                    # check is edge
                    if (self.server.node.check_max()):
                        reply = "you_next：" + json.dumps(self.server.node.next)
                        socket.sendto(reply.encode('utf-8'), targe)
                        reply = "you_pred：" + json.dumps(info)
                        socket.sendto(reply.encode('utf-8'), (self.server.node.next[1], self.server.node.next[2]))
                        self.server.node.next = info
                    else:
                        #  new node between next and self
                        if (info[0]<self.server.node.next[0]):
                            reply = "you_next：" + json.dumps(self.server.node.next)
                            socket.sendto(reply.encode('utf-8'), targe)
                            reply = "you_pred：" + json.dumps(info)
                            socket.sendto(reply.encode('utf-8'), (self.server.node.next[1], self.server.node.next[2]))
                            self.server.node.next = info
                        else:
                        #  new node big next, ask next
                            reply = "ask：" + json.dumps(self.server.node.next)
                            socket.sendto(reply.encode('utf-8'), targe)
                else:

                    reply = "ask：" + json.dumps(table)
                    socket.sendto(reply.encode('utf-8'), targe)
        if data_array[0] == "ask" :
            # socket.sendto(, "find_successor：" + json.dumps(msg))
            targe = (info[1], info[2])
            reply = "find_successor：" + json.dumps(self.server.node.info)
            socket.sendto(reply.encode('utf-8'), targe)

        if data_array[0] == "you_next":
            self.server.node.next = info
        if data_array[0] == "you_pred":
            self.server.node.pred = info

        if data_array[0] == "you_finger":
            self.server.node.finger[info[0]] = [info[1], info[2], info[3]]

        if data_array[0] == "get_your_next":
            targe = (info[1], info[2])
            reply = "you_next：" + json.dumps(self.server.node.next)
            socket.sendto(reply.encode('utf-8'), targe)
            reply = "you_pred：" + json.dumps(info)
            socket.sendto(reply.encode('utf-8'), (self.server.node.next[1], self.server.node.next[2]))
            self.server.node.next = info

        if data_array[0] == "is_me":
            if (self.server.node.pred[0] == info[0]):

                targe = (info[1], info[2])
                socket.sendto("update_finger：".encode('utf-8'), targe)
            else:
                targe = (info[1], info[2])
                reply = "you_next：" + json.dumps(self.server.node.pred)
                socket.sendto(reply.encode('utf-8'), targe)
                targe = (self.server.node.pred[1], self.server.node.pred[2])
                reply = "you_pred：" + json.dumps(info)
                socket.sendto(reply.encode('utf-8'), targe)

        if data_array[0] == "download":
            if (len(info) == 3):
                file_name = info[0]
                file = info[1]
                save(file_name, file, self.server.node.dir)
                self.server.node.file_list[info[2]] = info[0]
            else:
                file_name = info[0]
                dir = info[2]
                file = info[1]
                save(file_name, file, dir)
                self.server.node.file_list[info[3]] = info[0]

        if data_array[0] == "is_successor":
            file_name,content = read_file(data_array[2])
            file = [file_name, content.decode("utf-8"), file_name2id(get_file_name(data_array[2]))]
            reply = "download：" + json.dumps(file)
            socket.sendto(reply.encode('utf-8'), (info[1],info[2]))
            print("file save in "+ str(info[0]))

        if data_array[0] == "in_successor":

            targe = (info[1], info[2])
            source_info = json.loads(data_array[2])
            reply = "check_file：" + json.dumps(source_info)
            socket.sendto(reply.encode('utf-8'), targe)

        if data_array[0] == "check_file":

            is_find = self.server.node.check_file(info[0])

            if is_find is not None:
                file_path = is_find
                file_name, content = read_file(file_path)
                file = [file_name, content.decode("utf-8"),info[0]]
                reply = "download：" + json.dumps(file)
                socket.sendto(reply.encode('utf-8'), (info[1], info[2]))
            else:
                reply = "do_not_find：1"
                socket.sendto(reply.encode('utf-8'), (info[1],info[2]))

        if data_array[0] == "do_not_find":
            print("do not find file")

        if data_array[0] == "get_successor":
            targe = (info[1], info[2])
            if (info[0] < self.server.node.id and info[0] > self.server.node.pred[0]):
                reply = "is_successor：" + json.dumps(self.server.node.info)+"："+info[3]
                socket.sendto(reply.encode('utf-8'), targe)
            elif (info[0] > self.server.node.id and info[0] < self.server.node.next[0]):
                reply = "is_successor：" + json.dumps(self.server.node.next)+"："+info[3]
                socket.sendto(reply.encode('utf-8'), targe)
            else:
                successor = self.server.node.find_successor(info[0])
                if (successor[0] == self.server.node.id):
                    reply = "is_successor：" + json.dumps(self.server.node.next)+"："+info[3]
                    socket.sendto(reply.encode('utf-8'), targe)
                else:
                    reply = "get_successor：" + json.dumps(info)
                    socket.sendto(reply.encode('utf-8'), (successor[1],successor[2]))

        if data_array[0] == "serch_successor":
            targe = (info[1], info[2])
            if (info[0] < self.server.node.id and info[0] > self.server.node.pred[0]):
                reply = "in_successor：" + json.dumps(self.server.node.info)+"："+json.dumps(info)
                socket.sendto(reply.encode('utf-8'), targe)
            elif (info[0] > self.server.node.id and info[0] < self.server.node.next[0]):
                reply = "in_successor：" + json.dumps(self.server.node.next)+"："+json.dumps(info)
                socket.sendto(reply.encode('utf-8'), targe)
            else:
                successor = self.server.node.find_successor(info[0])
                if (successor[0] == self.server.node.id):
                    reply = "in_successor：" + json.dumps(self.server.node.next)+"："+json.dumps(info)
                    socket.sendto(reply.encode('utf-8'), targe)
                else:
                    reply = "serch_successor：" + json.dumps(info)
                    socket.sendto(reply.encode('utf-8'), (successor[1], successor[2]))

                # reply = "is_successor：" + json.dumps(self.server.node.next)
                # socket.sendto(reply.encode('utf-8'), targe)

