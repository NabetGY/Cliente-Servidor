import zmq
import os
import hashlib

# BUF_SIZE is totally arbitrary, change for your app!
BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

context = zmq.Context()
s = context.socket(zmq.REP)
s.bind('tcp://*:8001')

opcion = s.recv_string()
s.send_string('ok')
if(opcion=='upload'):
    user = s.recv_string()
    s.send_string('ok')
    file = s.recv_string()
    s.send_string('ok')
    with open(file, 'wb') as f:
        byte = s.recv_multipart()
        f.write(byte[0])
elif (opcion == 'download'):
    user = s.recv_string()
    s.send_string('ok')
    file = s.recv_string()
    with open(file, "rb") as f:
        byte = f.read()
        s.send_multipart([byte])
elif (opcion == 'list'):
    user = s.recv_string()
    data = os.listdir('.')
    listFiles = '\n'.join(data)
    s.send_string(listFiles)
elif (opcion == 'sharelink'):
    user = s.recv_string()
    s.send_string('ok')
    file = s.recv_string()
    md5 = hashlib.md5()
    with open(file, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)
        link = md5.hexdigest()
        s.send_string(link)
elif (opcion == 'downloadlink'):
    user = s.recv_string()
    s.send_string('ok')
    link = s.recv_string()
    files = os.listdir('.')
    for file in files:
        md5 = hashlib.md5()
        with open(file, "rb") as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)
        
        if link == md5.hexdigest():
            print('aqui toy')
            s.send_string(file)
            s.recv_string()
            with open(file, "rb") as f:
                byte = f.read()
                s.send_multipart([byte])


    