import pickle
import sys
from os import path
from datetime import datetime, date

class KeyValueStorage(object):
    '''
    Key-Value Store:
    Save as Dict[int]:list(value, timestamp)

    On write:
    save the whole dict to pickle file on write

    On initiation:
    if legal pickle file exist, init the obj by the pickle file
    else create empty dict
    
    # Warning:
    The naming of the pickle file maybe adapted to the server:port value, so the func restore() won't
    read wrong file
    '''
    def __init__(self, rep_id):
        self._store = dict()
        self.keyspace = 256
        
        self.replica_server =0
        self.id = rep_id
        self.logname = 'log_'+str(rep_id)+'.p'
        if path.exists(self.logname):
            self.restore(self.logname)

    def get(self, key):
        # Check key range
        print(self._store.keys())
        self._checkrange(key)
        # get corresponding value
        return self._store[key]

    def put(self, key, value):
        # write value to the key and log the dict
        if key in self._store.keys():
            t = datetime.now()
            t = t.strftime("%H:%M:%S:%f")
            self._store[key][0] = value
            self._store[key][1] = t

        else:
            t = datetime.now()
            t = t.strftime("%H:%M:%S:%f")
            self._store[key] = [value, t]
        self.logwrite(self.logname)

    def logwrite(self, logfile):
        # On write, save dict into pickle
        pickle.dump(self._store, open(logfile, 'wb'))

    def restore(self, logfile):
        # On obj init, if log file exist, restore the dict by the pickle file
        data = pickle.load(open(logfile, 'rb'))
        print('The data restored from' + logfile)
        print(data)
        self._store = data

    def _checkrange(self, key):
        assert (key in self._store.keys()),"Key is out of the replica's key space!"
