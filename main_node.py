import socket
from threading import Thread, Lock
import sys
import os
import time


class message():
    def __init__(self, msg, id, type, original_sender, msgfrom, proposed_serialNumber, proposed_processNumber) -> None:
        self.msg = msg
        self.id = id
        self.type = type  # reply message/ send message
        self.original_sender = original_sender
        self.msgfrom = msgfrom
        self.proposed_serialNumber = proposed_serialNumber
        self.proposed_processNumber = proposed_processNumber
        return

    def msg2text(self):
        return ":".join([self.msg, self.id, self.type, self.original_sender, self.msgfrom, self.proposed_serialNumber, self.proposed_processNumber, "\n"]).strip('\n')
# message format: DATA/REPLY/ACK:NAME:<Transaction>


class node():
    def __init__(self) -> None:
        self.queue = [] # [[message,serial_number,process_number,deliver_status]]
        # {"msg":transaction,"id":id,"from":self.PORT,"deliver":False,"priority":[serial_count,process],"count":0}
        self.account_dict = {}
        self.group = {"num_nodes":0, "nodes":[]} # {"num_nodes": 0, "nodes": [[name,address,port,client_socket,server_socket],...,]}
        self.account_lock = Lock()
        self.group_lock = Lock()
        self.q_lock = Lock()
        self.receive_pool_lock = Lock()
        self.n_connected = 0
        self.serial_count=0
        self.start_connect_time = None
        self.is_dead = False
        self.received_pool=[]
        self.stucked_msg = None
        self.stucked_timestamp = None
        
        self.init_connection()
        Thread(target=self._failer).start()
        Thread(target=self.send).start()
        for i in range(self.group['num_nodes']):
            Thread(target=self.receive,args= (self.group['nodes'][i][3],)).start()
        return  

    def read_config(self, path):
        fp = open(path, 'r')
        content = fp.readlines()
        self.group['num_nodes'] = int(content[0].strip("\n"))
        for i in range(1, len(content)):
            line = content[i].split(" ")
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.group['nodes'].append(
                [line[0], line[1], int(line[2].strip("\n")), client_socket])
        return

    def _failer(self):  # XXX kill node
        # if(self.NAME == "node1" or self.NAME == "node2" or self.NAME == "node3"): #case 4
        # if(self.NAME == "node1"):  # case 3
        if(self.NAME == "" ): #case 1 or 2
            dead_t = 10
        else:
            dead_t = 100
        while(True):
            # return
            if(self.start_connect_time == None):
                pass
            else:
                if(time.time()-self.start_connect_time >= dead_t):
                    self.is_dead = True
                    print(f"{self.NAME} fail")
                    self.q_lock.acquire()
                    self.log_deliver.write(
                        f'{time.time()-self.start_connect_time}\n'.encode())
                    for i in range(self.n_connected):
                        self.group['nodes'][i][3].close()
                        self.group['nodes'][i][4].close()
                    self.q_lock.release()
                    # exit(0)
                    break
        
        return

    def _connect(self):
        while(True):
            conn, addr = self.server_socket.accept()
            self.n_connected += 1
            # Thread(target=self.send,args=(conn,)).start()
            client_data = int(conn.recv(1024).decode())
            for i in range(self.group['num_nodes']):
                if(client_data == self.group['nodes'][i][2]):
                    self.group_lock.acquire()
                    self.group['nodes'][i].append(conn)
                    self.group['nodes'][i].append(True)
                    self.group_lock.release()
                    break
            if(self.n_connected == self.group['num_nodes']):
                break
        print("all connected")
        return

    def init_connection(self):
        self.NAME = sys.argv[1]
        self.PORT = int(sys.argv[2])
        self.config_path = sys.argv[3]

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(("localhost", self.PORT))
        self.server_socket.listen(10)
        # self.stucked_msg = None
        # self.stucked_timestamp = None

        self.log_deliver = open(
            f"logs/{self.NAME}_deliver.txt", "wb", buffering=0)

        self.read_config(self.config_path)
        Thread(target=self._connect).start()

        for i in range(len(self.group['nodes'])):
            temp_port = self.group['nodes'][i][2]
            temp_address = self.group['nodes'][i][1]
            while(1):
                try:
                    self.group['nodes'][i][3].connect(
                        (temp_address, temp_port))
                    self.group['nodes'][i][3].sendall(str(self.PORT).encode())
                    break
                except Exception as e:
                    pass
        while(1):
            if(self.n_connected == self.group['num_nodes']):
                self.start_connect_time = time.time()
                break
        # Thread(target=self._failer).start()  # XXX use _failer
        # Thread(target=self.send).start()
        return

    def send(self):
        while(self.n_connected != self.group['num_nodes']):
            pass
        while(True):
            for line in sys.stdin:
                if(self.is_dead == True):
                    return
                self.q_lock.acquire()
                msg_infoList = [line.strip("\n"),
                                str(self.PORT)+'#' +
                                str(time.time()),  # message_id
                                'send',
                                str(self.NAME),
                                str(self.PORT),
                                str(self.serial_count+1),
                                str(self.PORT), "\n"]
                msg = ':'.join(msg_infoList)
                msg_info = message(msg_infoList[0], msg_infoList[1], msg_infoList[2],
                                   msg_infoList[3], msg_infoList[4], msg_infoList[5], msg_infoList[6])
                self.queue.append({"msg": msg_info.msg, "id": msg_info.id, "from": self.PORT,
                                  "deliver": False, "priority": [self.serial_count+1, self.PORT], "count": 0})
                self.serial_count += 1
                self.q_lock.release()

                self.receive_pool_lock.acquire()
                self.received_pool.append(msg_info.msg2text())
                self.receive_pool_lock.release()

                self.multicast(msg)

        return

    def unicast(self, sock: socket.socket, msg: str):
        try:
            # print(self.queue)
            sock.sendall(msg.encode())

        except socket.error as e:
            for i in range(self.group['num_nodes']):
                self.group_lock.acquire()
                # XXX node fail detect
                if(self.group['nodes'][i][4] == sock and self.group['nodes'][i][5] == True):
                    print("Lost connection to", self.group['nodes'][i][0])
                    self.group['nodes'][i][5] = False
                    self.n_connected -= 1
                    sock.close()
                    self.group_lock.release()
                    break
                else:
                    self.group_lock.release()
                    continue
        return

    def multicast_msg(self, msg_info: message):
        msg = ':'.join([msg_info.msg,
                        msg_info.id,
                        msg_info.type,
                        msg_info.original_sender,
                        msg_info.msgfrom,
                        msg_info.proposed_serialNumber,
                        msg_info.proposed_processNumber, "\n"])
        self.multicast(msg)
        return

    def multicast(self, msg: str):
        for i in range(self.group['num_nodes']):
            self.unicast(self.group['nodes'][i][4], msg)
        return

    def receive(self, sock: socket.socket):
        while(True):
            if(self.is_dead):
                return
            try:
                msg = sock.recv(1024).decode()
                if(msg == ""):
                    return
                    # continue
                raw_msg = msg.split('\n')
                for i in range(len(raw_msg)-1):
                    msg_infoList = raw_msg[i].split(':')
                    begin = msg_infoList[0].split(" ")[0]
                    if(begin != "TRANSFER" and begin != "DEPOSIT"):
                        # print(msg_infoList)
                        continue
                    msg_info = message(msg_infoList[0], msg_infoList[1], msg_infoList[2],
                                       msg_infoList[3], msg_infoList[4], msg_infoList[5], msg_infoList[6])
                    self._receive(msg_info)
            except socket.error as e:
                print("receive", e)
        return

    def update_balance(self, trans):
        trans_list = trans['msg'].split(" ")
        trans_id = trans['id']
        if(len(trans_list) == 3):
            # DEPOSIT
            uid = trans_list[1]
            money = int(trans_list[2])
            if(uid in self.account_dict):
                self.account_dict[uid] += money
            else:
                self.account_dict[uid] = money

            # print(f"[SUCCESS] {trans['msg']} [BALANCE] {uid}: {self.account_dict[uid]}") #XXX print balance
            print(f"[SUCCESS] {trans['msg']}")
            print(f"[BALANCE] {self.get_balence_msg()}\n")
            self.log_deliver.write(
                f"[SUCCESS] {trans['msg']}:{trans_id}:{time.time()}\n".encode())

        else:
            # TRANSFOR
            id_from = trans_list[1]
            id_to = trans_list[3]
            money = int(trans_list[4])

            if(id_from not in self.account_dict):
                print(f"[IGNORE] {trans['msg']}\n")
                self.log_deliver.write(
                    f"[IGNORE] {trans['msg']}:{trans_id}:{time.time()}\n".encode())
                return
            elif(self.account_dict[id_from] < money):
                print(f"[IGNORE] {trans['msg']}\n")
                self.log_deliver.write(
                    f"[IGNORE] {trans['msg']}:{trans_id}:{time.time()}\n".encode())
                return
            else:
                if(id_to in self.account_dict):
                    self.account_dict[id_to] += money
                    self.account_dict[id_from] -= money
                    # print(f"[SUCCESS] {trans['msg']} [BALANCE] {id_from}: {self.account_dict[id_from]}, {id_to}: {self.account_dict[id_to]}")
                    print(f"[SUCCESS] {trans['msg']}")
                    print(f"[BALANCE] {self.get_balence_msg()}\n")
                    self.log_deliver.write(
                        f"[SUCCESS] {trans['msg']}:{trans_id}:{time.time()}\n".encode())
                else:
                    self.account_dict[id_to] = money
                    self.account_dict[id_from] -= money

                    # print(f"[SUCCESS] {trans['msg']} [BALANCE] {id_from}: {self.account_dict[id_from]}, {id_to}: {self.account_dict[id_to]}")
                    # print(f"[SUCCESS] {trans['msg']}")
                    # print(f"[BALANCE] {self.get_balence_msg()}\n")
                    self.log_deliver.write(
                        f"[SUCCESS] {trans['msg']}:{trans_id}:{time.time()}\n".encode())

    def get_balence_msg(self):
        balance_msg = ''
        for k in sorted(self.account_dict.keys()):
            if self.account_dict[k] != 0:

                balance_msg += f'{k}: {self.account_dict[k]}, '
        balance_msg = balance_msg[0:-2]
        return balance_msg

    # def deliver(self):
    #     while(1):
    #         if(len(self.queue) <= 0):
    #             break
    #         if(self.queue[0]['count'] >= self.n_connected):
    #             self.queue[0]['deliver'] = True
    #         if(self.queue[0]['deliver'] == True):
    #             trans = self.queue.pop(0)
    #             # print(trans)
    #             # print(self.n_connected)
    #             self.update_balance(trans)
    #         else:
    #             break
    #     return

    def deliver(self):
        while(1):
            if(len(self.queue) <= 0):
                break
            if(self.queue[0]['count']>=self.n_connected and self.queue[0]['deliver'] == False):
                print("Get unfind agree")
                self.queue[0]['deliver']=True
            if(self.queue[0]['deliver'] == True):
                trans = self.queue.pop(0)
                # print(trans)
                self.update_balance(trans)
            else:
                # print(self.queue[0])
                for i in range(self.group['num_nodes']):
                    if(self.group['nodes'][i][2] == self.queue[0]['from'] and self.group['nodes'][i][5] == False):
                        # print("Stuck:",self.queue[0])
                        if(self.stucked_msg == None):
                            self.stucked_msg = self.queue[0]['id']
                            self.stucked_timestamp = time.time()
                        elif(self.stucked_msg != self.queue[0]['id']):
                            self.stucked_msg = self.queue[0]['id']
                            self.stucked_timestamp = time.time()
                        else: 
                            if(time.time() - self.stucked_timestamp >= 6):
                                self.queue.pop(0)
                break
        return

    def find_socket(self, PORT):
        for node in self.group['nodes']:
            if node[2] == PORT:
                return node[4]

    def is_received(self, msg_info: message):
        if(msg_info.msg2text() in self.received_pool):
            return True
        else:
            return False

    def update_queue(self, msg_info: message):  # XXX update_queue func
        find = False
        for i in range(len(self.queue)):
            if(self.queue[i]['id'] == msg_info.id):
                find = True
                break
            else:
                continue

        if(not find):
            print("not find queue")

        if(self.queue[i]['priority'] <= [int(msg_info.proposed_serialNumber), int(msg_info.proposed_processNumber)]):
            self.queue[i]['priority'] = [
                int(msg_info.proposed_serialNumber), int(msg_info.proposed_processNumber)]
        self.queue[i]['count'] += 1

        # for i in range(len(self.queue)):
        #     if(self.queue[i]['count'] >=self.n_connected ):
        #         new_msg_info=[msg_info.msg,
        #                 msg_info.id,  #message_id
        #                 'agree',
        #                 msg_info.original_sender,
        #                 str(self.PORT),
        #                 str(self.queue[i]['priority'][0]),
        #                 str(self.queue[i]['priority'][1]),"\n"]
        #         self.multicast(":".join(new_msg_info))
        #         self.queue[i]['deliver'] = True
        # self.queue.sort(key = lambda x:x['priority'])
        # self.deliver()

        if(self.queue[i]['count'] >= self.n_connected):
            new_msg_info = [msg_info.msg,
                            msg_info.id,  # message_id
                            'agree',
                            msg_info.original_sender,
                            str(self.PORT),
                            str(self.queue[i]['priority'][0]),
                            str(self.queue[i]['priority'][1]), "\n"]
            # str(self.queue[i]['priority'][1]),"\n"]
            self.queue[i]['deliver'] = True
            self.queue.sort(key=lambda x: x['priority'])
            self.deliver()

            self.receive_pool_lock.acquire()
            self.received_pool.append(":".join(new_msg_info).strip('\n'))
            self.receive_pool_lock.release()

            self.multicast(":".join(new_msg_info))

        return

    def agree_queue(self, msg_info: message):
        find = False
        for i in range(len(self.queue)):
            if(self.queue[i]['id'] == msg_info.id):
                find = True
                # print(f"find agree {msg_info.msg2text()}")
                break
            else:
                continue
        if(not find):
            print(f"not find agree{msg_info.msg2text()}ending")
            print(self.received_pool)

        self.queue[i]['priority'] = [
            int(msg_info.proposed_serialNumber), int(msg_info.proposed_processNumber)]
        self.queue[i]['deliver'] = True
        self.queue[i]['count'] = self.n_connected
        self.queue.sort(key=lambda x: x['priority'])
        self.deliver()

        return

    def _receive(self, msg_info: message):
        self.receive_pool_lock.acquire()
        if(self.is_received(msg_info)):
            self.receive_pool_lock.release()
            return
        else:
            self.received_pool.append(msg_info.msg2text())
            self.receive_pool_lock.release()
            self.multicast_msg(msg_info)

        if(msg_info.type == 'send'):
            if(msg_info.msgfrom == self.PORT):
                return
            self.q_lock.acquire()
            self.queue.append({"msg": msg_info.msg, "id": msg_info.id, "from": int(
                msg_info.msgfrom), "deliver": False, "priority": [self.serial_count+1, self.PORT], "count": 0})
            new_msg_info = [msg_info.msg,
                            msg_info.id,
                            'reply',
                            msg_info.original_sender,
                            str(self.PORT),
                            str(self.serial_count+1),
                            str(self.PORT), "\n"]
            self.serial_count += 1
            self.q_lock.release()

            self.receive_pool_lock.acquire()
            self.received_pool.append(":".join(new_msg_info).strip('\n'))
            self.receive_pool_lock.release()

            self.multicast(":".join(new_msg_info))

        elif msg_info.type == 'reply':
            # if(self.is_received):
            #     pass
            # self.multicast()
            if(msg_info.original_sender == self.NAME):
                self.q_lock.acquire()
                self.update_queue(msg_info)  # XXX where queue is locked
                self.q_lock.release()

        elif(msg_info.type == 'agree'):
            if(msg_info.msgfrom == self.PORT):
                return
            self.q_lock.acquire()
            self.agree_queue(msg_info)
            self.q_lock.release()


if __name__ == "__main__":
    # log_path=f'logs/{sys.argv[1]}_deliver.txt'
    # if  os.path.exists(log_path):
    #     os.remove(log_path)
    nodes = node()
    while(True):
        pass
