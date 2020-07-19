from  utils import *
import threading
from listen import Listen_Server, Handler
import json, time, os

class Node:
    def __init__(self, ip, port, guide_ip=None, guide_port=None):
        self.ip = ip
        self.port = port
        self.address = ip + " " + str(port)
        self.finger = dict()
        self.id = add2id(self.address)
        # self.info = str(self.id) + " " + self.address
        self.info = [self.id, self.ip, self.port]
        self.next = self.info
        self.pred = self.info
        self.next_alive = False
        self.pred_alive = False
        self.next_alive_count = 1
        self.pred_alive_count = 1
        self.stabilize_on = True
        if guide_ip:
            self.guid_ip = guide_ip
            self.guid_port = guide_port
            self.guid_id = add2id(guide_ip + " " + str(guide_port))
        else:
            # first node
            self.guid_id = None

        self.file_list = dict()

        self.server = Listen_Server((self.ip, self.port), Handler, self)
        listen = threading.Thread(target=self.server.serve_forever, args=())

        self.command = threading.Thread(target=self.wait_com, args=(self,))
        self.stabilize = threading.Thread(target=self.stabilize, args=())
        heart = threading.Thread(target=self.heart, args=())
        alive = threading.Thread(target=self.alive, args=())
        listen.start()
        self.command.start()
        self.init_finger()
        self.init_default_dir()
        self.stabilize.start()
        heart.start()
        alive.start()
    def stabilize(self):
        while (self.stabilize_on):
            time.sleep(5)
            if(self.next_alive):
                reply = "is_me：" + json.dumps(self.info)
                send_msg(self.next[1], self.next[2], reply)
        if(self.stabilize_on is False):
            msg = "you_pred：" + json.dumps(self.pred)
            send_msg(self.next[1], self.next[2], msg)
            msg = "you_pred_dead：1"
            send_msg(self.next[1], self.next[2], msg)
            msg = "you_next：" + json.dumps(self.next)
            send_msg(self.pred[1], self.pred[2], msg)
            msg = "you_next_dead：1"
            send_msg(self.pred[1], self.pred[2], msg)


    def init_finger(self):
        for i in range(26):
            self.finger[i] = self.info
        if self.guid_id:
            send_msg(self.guid_ip, self.guid_port, "join："+json.dumps(self.info))

    def init_default_dir(self):

        dir = "./dir_"+str(self.id)+"/"
        if not os.path.exists(dir):
            os.makedirs(dir)
        self.dir = dir

    def heart(self):
        last_pred = 0
        last_next = 0
        while (self.stabilize_on):
            time.sleep(10)
            if (last_pred == self.pred_alive_count):
                self.pred_alive = False
                if(self.pred_alive_count>9999999):
                    self.pred_alive_count = 0
            last_pred = self.pred_alive_count

            if (last_next == self.next_alive_count):
                self.next_alive = False
                if(self.next_alive_count>9999999):
                    self.next_alive_count = 0
            last_next = self.next_alive_count
    def alive(self):
        while (self.stabilize_on):
            time.sleep(5)
            msg = "you_pred_alive："+json.dumps(self.info)
            send_msg(self.next[1], self.next[2], msg)
            msg = "you_next_alive："+json.dumps(self.info)
            send_msg(self.pred[1], self.pred[2], msg)
    def update_finger(self):
        if (self.next[0] != self.id):
            for i in range(len(self.finger)):
                uid = self.id+2**i
                msg = [i, uid, self.ip, self.port]
                if (self.check_max()):
                    if (compar(uid, self.id)):
                        self.finger[i] = self.next
                    elif(compar(self.next[0], uid)):
                        self.finger[i] = self.next
                    else:
                        send_msg(self.next[1], self.next[2], "find_successor：" + json.dumps(msg))
                else:
                    if (compar(self.next[0], uid)):
                        self.finger[i] = self.next
                    if (compar(uid, self.next[0])):
                        send_msg(self.next[1], self.next[2], "find_successor：" + json.dumps(msg))


    def find_successor(self, id):
        if ( compar(id, self.id)):
            if (compar(self.next[0], id) or self.check_max()):
                return  self.info
            else:
                self.update_finger()
                last_id = self.id
                isround = False
                for i in range(len(self.finger)):
                    if (last_id > self.finger[i][0]):
                        isround = True
                    if(isround is True):
                        break;
                    if (compar(self.finger[i][0], id)):
                        break
                    last_id = self.finger[i][0]

                if (i == 0):
                    return self.info
                else:
                    return self.finger[i - 1]

        else:
            if(compar(id, self.server.node.pred[0]) or self.server.node.check_min()):
                return  self.pred
            else:
                last_id = self.id
                isround = False
                for i in range(len(self.finger)):
                    if (isround is False):
                        if(last_id>self.finger[i][0]):
                            isround = True
                    if (compar(self.finger[i][0], id) and isround):
                        break
                    last_id = self.finger[i][0]
                if(i == 0):
                    return self.info
                else:
                    return self.finger[i-1]


    def check_max(self):
        if (self.next[0] < self.id):
            return True
        else:
            return False

    def check_min(self):
        if (self.pred[0] > self.id):
            return True
        else:
            return False

    def exit(self):
        self.stabilize_on = False
        # msg = "you_pred：" + json.dumps(self.pred)
        # send_msg(self.next[1], self.next[2], msg)
        # msg = "you_pred_dead：1"
        # send_msg(self.next[1], self.next[2], msg)
        # msg = "you_next：" + json.dumps(self.next)
        # send_msg(self.pred[1], self.pred[2], msg)
        # msg = "you_next_dead：1"
        # send_msg(self.pred[1], self.pred[2], msg)
        self.server.shutdown()

    def get_file_successor(self):
        file_path = input("Enter file path:\n")
        fid = file_name2id(get_file_name(file_path))
        if (self.next[0] is not self.id):
            targe = self.find_successor(fid)
            if (targe[0]!=self.id):
                send_msg(targe[1], targe[2], "get_successor：" + json.dumps([fid,self.ip,self.port])+"："+file_path)
            else:
                msg = "is_successor：" + json.dumps(self.next)+"："+ file_path
                send_msg(self.ip, self.port, msg)
        else:
            msg = "is_successor：" + json.dumps(self.next) + "：" + file_path
            send_msg(self.ip, self.port, msg)
    def serch_file(self):
        file_name = input("Enter file name:\n")
        fid = file_name2id(get_file_name(file_name))
        targe = self.find_successor(fid)
        # print("the file fid is:" + str(fid))
        # print(targe)
        # if (targe[0]!=self.id):
        send_msg(targe[1], targe[2], "serch_successor：" + json.dumps([fid,self.ip,self.port]))
        # else:
        #     result = self.check_file(fid)
        #     if (result is not None):


    def check_file(self, fid):
        if(self.file_list.get(fid) is not None):
            return self.file_list.get(fid)
        else:
            return None


    def wait_com(self, node):
        print("use help to get more information.")
        while True:
            print()
            command = input("Please enter command:\n")
            if command == 'exit':
                node.exit()
                break
            if command == 'next':
                print(node.next)
                print("alive: "+str(node.next_alive))
            if command == 'pred':
                print(node.pred)
                print("alive: "+str(node.pred_alive))
            if command == 'finger':
                for key, value in self.finger.items():
                    print('{key}:{value}'.format(key=key, value=value))
            if command == 'self':
                print(node.info)
            if command == 'upload':
                self.get_file_successor()
            if command == 'file':
                print(self.file_list)
            if command == 'search':
                self.serch_file()
            if command == 'help':
                print("Command list:")
                print("next: Output information of own successor nodes.")
                print("pred: Output the information of its own predecessor node.")
                print("self: Output current node information.")
                print("file: Output local resources.")
                print("finger: Output the finger table of the current node.")
                print("upload: Upload the local resources of the current node to the network.")
                print("search: Search for this resource in the network.")
                print("exit: Let the current node exit the network.")

    def get_finger(self, idx):
        return self.finger[idx]