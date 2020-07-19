import socketserver
import json
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
            # There are at least two nodes in the current network
            if (self.server.node.next[0] != self.server.node.id):

                if(id > self.server.node.id):

                    if (id < self.server.node.next[0] or self.server.node.check_max()):
                        reply = "you_next：" + json.dumps(self.server.node.next)
                        socket.sendto(reply.encode('utf-8'), targe)
                        reply = "you_pred：" + json.dumps(self.server.node.info)
                        socket.sendto(reply.encode('utf-8'), targe)
                        reply = "you_pred：" + json.dumps(info)
                        socket.sendto(reply.encode('utf-8'), (self.server.node.next[1],self.server.node.next[2]))
                        self.server.node.next = info
                    else:
                        nearest = self.server.node.find_successor(id)
                        reply = "join：" + json.dumps(info)
                        socket.sendto(reply.encode('utf-8'), (nearest[1], nearest[2]))
                else:
                    if(id > self.server.node.pred[0] or self.server.node.check_min()):
                        reply = "you_next：" + json.dumps(self.server.node.info)
                        socket.sendto(reply.encode('utf-8'), targe)
                        reply = "you_pred：" + json.dumps(self.server.node.pred)
                        socket.sendto(reply.encode('utf-8'), targe)
                        reply = "you_next：" + json.dumps(info)
                        socket.sendto(reply.encode('utf-8'), (self.server.node.pred[1], self.server.node.pred[2]))
                        self.server.node.pred = info
                    else:
                        reply = "join：" + json.dumps(info)
                        socket.sendto(reply.encode('utf-8'), (self.server.node.pred[1], self.server.node.pred[2]))

            # There is only one node in the current network
            else:
                self.server.node.next = info
                self.server.node.pred = info
                self.server.node.update_finger()
                self.server.node.next_alive = True
                self.server.node.pred_alive = True
                self.server.node.update_finger()
                reply = "you_next：" + json.dumps(self.server.node.info)
                socket.sendto(reply.encode('utf-8'), targe)
                reply = "you_pred：" + json.dumps(self.server.node.info)
                socket.sendto(reply.encode('utf-8'), targe)
                socket.sendto("update_finger：".encode('utf-8'), targe)

        if data_array[0] == "update_finger" :
            self.server.node.update_finger()

        # Modify the message related to the finger list
        if data_array[0] == "find_successor" :
            if (compar(self.server.node.next[0], info[1]) or self.server.node.check_max()):
                targe = (info[2], info[3])
                table = self.server.node.next[:]
                table.insert(0, info[0])
                reply = "you_finger：" + json.dumps(table)
                socket.sendto(reply.encode('utf-8'), targe)
            else:
                table = self.server.node.find_successor(info[1])[:]
                targe = (table[1], table[2])
                reply = "find_successor：" + json.dumps(info)
                socket.sendto(reply.encode('utf-8'), targe)

        # Operate on the node's predecessor and successor nodes
        if data_array[0] == "you_next":
            self.server.node.next = info
        if data_array[0] == "you_pred":
            self.server.node.pred = info

        if data_array[0] == "you_finger":
            self.server.node.finger[info[0]] = [info[1], info[2], info[3]]

        # Stability related message
        if data_array[0] == "is_me":
            if (self.server.node.pred[0] == info[0]):
                targe = (info[1], info[2])
                socket.sendto("update_finger：".encode('utf-8'), targe)
            elif(info[0]<self.server.node.pred[0]):
                print(data)
                if (self.server.node.pred_alive):
                    targe = (info[1], info[2])
                    reply = "you_next：" + json.dumps(self.server.node.pred)
                    socket.sendto(reply.encode('utf-8'), targe)
                    targe = (self.server.node.pred[1], self.server.node.pred[2])
                    reply = "you_pred：" + json.dumps(info)
                    socket.sendto(reply.encode('utf-8'), targe)
                else:
                    self.server.node.pred = info
            else:
                if (self.server.node.pred_alive):
                    targe = (info[1], info[2])
                    reply = "you_pred：" + json.dumps(self.server.node.pred)
                    socket.sendto(reply.encode('utf-8'), targe)
                    targe = (self.server.node.pred[1], self.server.node.pred[2])
                    reply = "you_next：" + json.dumps(info)
                    socket.sendto(reply.encode('utf-8'), targe)
                    self.server.node.pred = info
                else:
                    self.server.node.pred = info

        if data_array[0] == "download":
            if (len(info) == 3):
                file_name = info[0]
                file = info[1]
                save(file_name, file, self.server.node.dir)
                print("get file "+ info[0])
                self.server.node.file_list[info[2]] = info[0]
            # Provide other parameters for download, download to the specified folder, but have not yet been implemented
            else:
                file_name = info[0]
                dir = info[2]
                file = info[1]
                save(file_name, file, dir)
                self.server.node.file_list[info[3]] = info[0]

        # The file should be saved in this node
        if data_array[0] == "is_successor":
            file_name,content = read_file(data_array[2])
            file = [file_name, content.decode("utf-8"), file_name2id(get_file_name(data_array[2]))]
            reply = "download：" + json.dumps(file)
            socket.sendto(reply.encode('utf-8'), (info[1],info[2]))
            print("file save in "+ str(info[0]))
        # Check whether the file is on the node
        if data_array[0] == "in_successor":
            targe = (info[1], info[2])
            is_find = self.server.node.check_file(info[0])
            if is_find is not None:
                file_path = is_find
                file_name, content = read_file(file_path)
                file = [file_name, content.decode("utf-8"),info[0]]
                reply = "download：" + json.dumps(file)
                socket.sendto(reply.encode('utf-8'), (targe))
            else:
                reply = "do_not_find：1"
                socket.sendto(reply.encode('utf-8'), (targe))


        if data_array[0] == "do_not_find":
            print("do not find file")

        if data_array[0] == "get_successor":
            table = self.server.node.find_successor(info[0])[:]
            if (table[0] is not self.server.node.id):
                targe = (table[1], table[2])
                reply = "get_successor：" + json.dumps(info)+"："+data_array[2]
                socket.sendto(reply.encode('utf-8'), targe)
            else:
                targe = (info[1], info[2])
                reply = "is_successor：" + json.dumps(self.server.node.next)+"："+data_array[2]
                socket.sendto(reply.encode('utf-8'), (targe))


        if data_array[0] == "serch_successor":
            table = self.server.node.find_successor(info[0])[:]
            if (table[0] is not self.server.node.id):
                targe = (table[1], table[2])
                reply = "serch_successor：" + json.dumps(info)
                socket.sendto(reply.encode('utf-8'), targe)
            else:
                reply = "in_successor：" + json.dumps(info)
                socket.sendto(reply.encode('utf-8'), (self.server.node.next[1], self.server.node.next[2]))

        if data_array[0] == "you_pred_alive":
            if(self.server.node.id != info[0]):
                self.server.node.pred_alive = True
                self.server.node.pred_alive_count += 1

        if data_array[0] == "you_next_alive":
            if (self.server.node.id != info[0]):
                self.server.node.next_alive = True
                self.server.node.next_alive_count += 1

        if data_array[0] == "you_pred_dead":
            self.server.node.pred_alive = False


        if data_array[0] == "you_next_dead":
            self.server.node.next_alive = False
