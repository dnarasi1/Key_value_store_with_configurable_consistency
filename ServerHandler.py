from time import sleep
import hashlib
import SocketServer
from struct import unpack
from struct import pack
import utility as util
import sys
import socket
sys.path.append('/home/yaoliu/src_code/protobuf-3.7.0/python')
import MessageType_pb2 as pb
from replica import Replica


class ReplicaServer(SocketServer.ThreadingTCPServer):
        def __init__(self, server_address,
                     RequestHandlerClass,
                     replica_id,
                     host,
                     port,
                     rep_list):
                SocketServer.ThreadingTCPServer.__init__(self,server_address,RequestHandlerClass)
                self.rep_id = replica_id
                self.hostname = host
		self.portno = port
                self.rep_lst = rep_list
                self.replica = Replica(self.rep_id, self.rep_lst)




class ServerHandler(SocketServer.BaseRequestHandler):

    def setup(self):
        self.host = self.server.hostname
        self.port = self.server.portno
        self.id = self.server.rep_id
        self.rep_lst = self.server.rep_lst
        # Init the replica obj
    def handle(self):
            counter = 0
            p_ip = self.client_address[0]
            p_port = self.client_address[1]
            print("Handling message from "+str(p_ip)+':'+str(p_port))
            while 1:
                    if counter > 10:
                            print('infinite while loop, break')
                            break
                    
                    message = self.request.recv(10000)
                    pb_msg = pb.Msg()
                    pb_msg.ParseFromString(message)
                    # Read from replica
                    if pb_msg.HasField('Get'):
                        rep_id = pb_msg.Get.id
                        key = pb_msg.Get.key
                        consistency = pb_msg.Get.consistencylevel
                        if rep_id == -1:
                                self.server.replica.read(key, consistency, self.request)
                        else:
                                self.server.replica.get_update(key, self.request)

                
                    # Read response
                    if pb_msg.HasField('GetRequestLst'):
                            rep_id = pb_msg.GetResponse.id
                            value = pb_msg.GetResponse.value
                            version = pb_msg.GetResponse.version
                            status = pb_msg.GetResponse.status
                            '''
                            Process response in local replica obj
                            '''

            
                    # Write value to replica
                    if pb_msg.HasField('Put'):
                            rep_id = pb_msg.Put.id
                            key = pb_msg.Put.key
                            value = pb_msg.Put.value
                            consistensy = pb_msg.Put.consistencylevel
                            if rep_id == -1:
                                    self.server.replica.write(key, value, consistensy, self.request)
                            else:
                                    # get hint from local replica
                                    if rep_id in self.server.replica.hint:
                                            print("Hint handoff")
                                            # send lost value back to coord
                                            h = self.server.replica.hint[rep_id]
                                            for i in h:
                                                    print("Send hint back to coord")
                                                    print(i)
                                                    msg = util.marsh_put(
                                                            rep_id,
                                                            i[0],
                                                            i[1],
                                                            consistensy  
                                                    )
                                                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # TCP
                                                    info = self.server.replica.rep_lst[rep_id-1]
                                                    sock.connect((info[1], info[2]))
                                                    sleep(1)
                                                    sock.sendall(msg)
                                                    sock.close()
                                                    
                                            del self.server.replica.hint[rep_id]
                                    print("Get hint")
                                    self.server.replica.update(key, value, self.request)
                    '''
                        # Write response
                        if pb_msg.HasField('PutResponse'):
                                rep_id = pb_msg.PutResponse.id
                                key = pb_msg.PutResponse.key
                                status = pb_msg.PutResponse.status
                    '''
                    counter += 1
if __name__ == '__main__':
    filename = sys.argv[1]
    cur_id = int(sys.argv[2])
    rep_lst = []
    cur_ip = 0
    cur_port = 0
    with open(filename, 'r') as f:
        for line in f:
            cmd = line.split(' ')
            rep_id = int(cmd[0])
            ip = str(cmd[1])
            port = int(cmd[2])
            if rep_id == cur_id:
                cur_id = int(rep_id)
                cur_ip = str(ip)
                cur_port = int(port)
            rep_lst.append([rep_id, ip, port])

    
    print('Starting replica server ' + str(cur_id) + ' on ' +\
          str(cur_ip) + ':' + \
          str(cur_port) + '...')

    print(rep_lst)
    server = ReplicaServer((str(cur_ip), int(cur_port)),
                               ServerHandler,
                               cur_id,
                               cur_ip,cur_port,
                               rep_lst)
    server.serve_forever()
