import socket
import pickle
from struct import pack, unpack

class SocketPool(object):
    def __init__(self, rep_id, rep_info):
        self.id = rep_id
        self.id_lst = [i[0] for i in rep_info]
        self.conn_info = [i[1] for i in rep_info]

    def initialize(self, preferred):
        self.sock_dict = dict()
        print(str(self.id) + " initialized the socketpool"+ "*"*10)
        print("Connected socket: ")
        for info in preferred:
            if self.id == info[0]:
                continue
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # TCP
                sock.connect(info[1])
                self.sock_dict[info[0]] = sock
                print("    "+str(info[0]))
            except:
                self.sock_dict[info[0]] = -1
            
    def send_msg(self, msg, sock):
        sock.sendall(msg)
        # response = sock.recv(10000)
        # return response

    def send_msg_pref(self, msg, consistency):
        resp_lst = []
        counter = 0
        for i,sock in self.sock_dict.items():
            print("counter: {}".format(counter))
            try:
                sock.sendall(msg)
                response = pickle.loads(sock.recv(10000))
                resp_lst.append([i]+response)
            except:
                counter += 1
                if consistency == 0:
                    if counter == 3:
                        # send err msg back to client
                        return -1
                        
                elif consistency == 1:
                    if counter == 2:
                        return -1


        return resp_lst
            
    def close_all(self):
        print("id: "+str(self.id)+" Closing socket")
        for i,sock in self.sock_dict.items():
            if self.id == i:
                continue
            try:
                sock.close()
            except:
                pass
            print("    socket id "+str(i) + " closed")
        
        
