import SocketServer
import pickle
from struct import unpack
from struct import pack
from collections import defaultdict
import sys
from KeyValueClass import KeyValueStorage
from socketpool import SocketPool
sys.path.append('/home/yaoliu/src_code/protobuf-3.7.0/python')
import MessageType_pb2 as pb
import utility as util


class Replica(object):
    def __init__(self, rep_id, rep_lst):
        self.N = 4
        self.id = rep_id
        self.rep_lst = rep_lst
        self.hint = defaultdict(list)
        self.data = KeyValueStorage(rep_id)
        # calculate the local pref lst and pass it in to socket pool
        self.preference = self.get_preferencelst(self.id, self.rep_lst)  # preference:[id, tuple:(ip,port)]
        # Init the sockpool with the prefferred replica info
        self.socketpool = SocketPool(self.id, self.preference)

    def get_preferencelst(self, rep_id, rep_lst):
        preference = []
        for i in range(3):
            if rep_id + i <= len(self.rep_lst):
                preference.append(
                    self.rep_lst[rep_id + i - 1]
                )
            else:
                preference.append(
                    self.rep_lst[(rep_id + i) % 4 - 1]
                )
            ret = [[j[0], (j[1],j[2])] for j in preference]
        return ret
    
    def read(self, key, consistency, sock):
        # Check if the is in the keyspace
        target_id = self.partition(key)
        # get target pref lst
        preferred = self.get_preferencelst(target_id, self.rep_lst)
        response = []
        if any(self.id in i for i in preferred):
            res = self.data.get(key)
            # Get data from preference list
            response.append([self.id]+ res)  # 0: id, 1: value, 2: time
            self.socketpool.initialize(preferred)
            msg = util.marsh_get(self.id, key, consistency)
            resp_lst = self.socketpool.send_msg_pref(msg, consistency)
            if resp_lst == -1:
                msg = util.marsh_err("Exception: Not enough replicas available")
                self.socketpool.send_msg(msg, sock)
                self.socketpool.close_all()
                return
            else:
                response += resp_lst
                # get the most recent 2 replica returns
                response.sort(key=lambda x:x[2], reverse=True)
                print(response)
                # Check consistency
                # Consistent value check
                util.check_consistency(response, key, consistency, preferred)
                response = response[:2]
                msg = util.marsh_getresp(response)
                self.socketpool.send_msg(msg, sock)
                self.socketpool.close_all()
        else:
            self.socketpool.initialize(preferred)
            msg = util.marsh_get(self.id, key, consistency)
            resp_lst = self.socketpool.send_msg_pref(msg, consistency)
            print(resp_lst)
            if resp_lst == -1:
                msg = util.marsh_err("Exception: Not enough replicas available")
                self.socketpool.send_msg(msg, sock)
                self.socketpool.close_all()
                return
            else:
                resp_lst.sort(key=lambda x:x[2], reverse=True)
                print(resp_lst)
                util.check_consistency(resp_lst, key, consistency, preferred)
                resp_lst = resp_lst[:2]
                msg = util.marsh_getresp(resp_lst)
                self.socketpool.send_msg(msg, sock)
                # Close all socket
                self.socketpool.close_all()
            

    def recv_readresponse(self):
        pass

    def write(self, key, value, consistency, sock):
        target_id = self.partition(key)
        # get target pref lst
        preferred = self.get_preferencelst(target_id, self.rep_lst)
        print(preferred)
        response = []
        if any(self.id in i for i in preferred):
            # Update self
            self.data.put(key, value)
            response.append([self.id, 'success'])  
            # Create Connection with the replica in the pref lst
            self.socketpool.initialize(preferred)
            msg = util.marsh_put(self.id, key, value, consistency)
            resp_lst = self.socketpool.send_msg_pref(msg, consistency)
            if resp_lst == -1:
                msg = util.marsh_err("Exception: Not enough replicas available")
                self.socketpool.send_msg(msg, sock)
                self.socketpool.close_all()
                return
            else: 
                response += resp_lst
                print(response)
                self.hinted_handoff(key, value, response, preferred)
                status = util.check_status(response, consistency)
                msg = util.marsh_putresp(self.id, key, status)
                self.socketpool.send_msg(msg, sock)
                # Close all socket
                self.socketpool.close_all()
        else:
            self.socketpool.initialize(preferred)
            msg = util.marsh_put(self.id, key, value, consistency)
            resp_lst = self.socketpool.send_msg_pref(msg, consistency)
            print(resp_lst)
            if resp_lst == -1:
                msg = util.marsh_err("Exception: Not enough replicas available")
                self.socketpool.send_msg(msg, sock)
                self.socketpool.close_all()
                return
            else:
                self.hinted_handoff(key, value, response, preferred)
                status = util.check_status(resp_lst, consistency)
                msg = util.marsh_putresp(self.id, key, status)
                self.socketpool.send_msg(msg, sock)
                # Close all socket
                self.socketpool.close_all()


    def update(self, key, value, sock):
        self.data.put(key, value)
        res = ["success"]
        ret = pickle.dumps(res)
        self.socketpool.send_msg(ret, sock)

    def get_update(self, key, sock):
        temp = self.data.get(key)
        ret = pickle.dumps(temp)
        self.socketpool.send_msg(ret, sock)
        

    def recv_writeresponse(self):
        pass

    def partition(self, key):
        if 0 <= key <= 63:
            self.replica_server = 1
        elif 64 <= key <= 127:
            self.replica_server = 2
        elif 128 <= key <= 191:
            self.replica_server = 3
        elif 192 <= key <= 255:
            self.replica_server = 4
        return self.replica_server


    def hinted_handoff(self, key, value, response, pref_lst):
        id_lst = [i[0] for i in response]
        if len(response) < 3:
            for i in pref_lst:
                if i[0] not in id_lst:
                    self.hint[i[0]].append((key, value))

if __name__ == '__main__':
    pass
