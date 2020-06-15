import sys
import socket
from KeyValueClass import KeyValueStorage
sys.path.append('/home/yaoliu/src_code/protobuf-3.7.0/python')

import MessageType_pb2 as pb

class clientRequest():
    def putReqHandler(self, key, value, consistency, s):
        msg = pb.Msg()
        msg.Put.id = -1
        msg.Put.key = key
        msg.Put.value = value
        msg.Put.consistencylevel =  consistency
        s.sendall(msg.SerializeToString())
        recv = s.recv(10000)
        pb_msg = pb.Msg()
        pb_msg.ParseFromString(recv)
        print(pb_msg)

    def getReqHandler(self, key, consistency, s):
        msg = pb.Msg()
        msg.Get.id = -1
        msg.Get.key = key
        msg.Get.consistencylevel =  consistency
        print("Sending get msg")
        s.sendall(msg.SerializeToString())
        recv = s.recv(1000)
        pb_msg = pb.Msg()
        pb_msg.ParseFromString(recv)
        print(pb_msg)
        

if __name__ == '__main__':
    ip = sys.argv[1]
    port = int(sys.argv[2])
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = (ip, port)
    s.connect(address)

    while 1:
        inputReq = raw_input("Enter the key and value:")
        request = inputReq.split()

        if request[0] == "Put":
            clientRequest().putReqHandler(int(request[1]), request[2], int(request[3]), s)

        if request[0] == "Get":
            clientRequest().getReqHandler(int(request[1]),int(request[2]), s)

       
    
