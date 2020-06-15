from struct import unpack
from struct import pack
import socket
import sys
import pickle
sys.path.append('/home/yaoliu/src_code/protobuf-3.7.0/python')
import MessageType_pb2 as pb


def marsh_get(rep_id, key, consistency):
    m_get = pb.Get()
    m_get.id = rep_id
    m_get.key = key
    m_get.consistencylevel = consistency
    msg = pb.Msg()
    msg.Get.CopyFrom(m_get)
    encoded = msg.SerializeToString()
    return encoded

def marsh_put(rep_id, key, value, consistency):
    m_put = pb.Put()
    m_put.id = rep_id
    m_put.key = key
    m_put.value = value
    m_put.consistencylevel = consistency
    msg = pb.Msg()
    msg.Put.CopyFrom(m_put)
    encoded = msg.SerializeToString()
    return encoded

def marsh_getresp(resp_lst):
    resp = pb.GetResponseLst()
    for i in resp_lst:
        m_getresp = resp.response.add()
        m_getresp.id = i[0]
        m_getresp.value = i[1]
        m_getresp.version = i[2]
    msg = pb.Msg()
    msg.GetRequestLst.CopyFrom(resp)
    encoded = msg.SerializeToString()
    return encoded

def marsh_putresp(rep_id, key, status):
    m_putresp = pb.PutResponse()
    m_putresp.id = rep_id
    m_putresp.key = key
    m_putresp.status = status
    msg = pb.Msg()
    msg.PutRequest.CopyFrom(m_putresp)
    encoded = msg.SerializeToString()
    return encoded

def marsh_err(err):
    m_err = pb.Err()
    m_err.err = err
    msg = pb.Msg()
    msg.err.CopyFrom(m_err)
    encoded = msg.SerializeToString()
    return encoded

def check_status(status_lst, consistency):
    status = [i[1] for i in status_lst]
    print(status)
    if consistency == 0:
        # One
        if status.count('success') >= 1:
            return "Success"
        else:
            return "Fail"
    else:
        # Quorum
        if status.count('success') >= 2:
            return "Success"
        else:
            return "Fail"

def read_repair(rep_id, key, c_val, pref_lst, consistency):
    idx = [i for i in range(len(pref_lst)) if pref_lst[i][0] == rep_id][0]
    conn_info = pref_lst[idx][1]
    msg = marsh_put(rep_id, key, c_val, consistency)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(conn_info)
    sock.send(msg)
    res = sock.recv(10000)
    print("read_repair:")
    print(res)
    sock.close()

        
def check_consistency(lst, key, consistency, pref_lst):
    correct = lst[0]
    c_val = correct[1]
    if consistency == 0:
        for i in range(1, len(lst)):
            if c_val != lst[i][1]:
                print('*'*10)
                print('Perform read repair')
                read_repair(lst[i][0], key, c_val, pref_lst, consistency)
                
    if consistency == 1:
        for i in range(2, len(lst)):
            if c_val != lst[i][1]:
                print('*'*10)
                print('Perform read repair')
                read_repair(lst[i][0], key, c_val, pref_lst, consistency)
                
