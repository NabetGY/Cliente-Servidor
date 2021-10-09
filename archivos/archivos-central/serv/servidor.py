import json
import zmq
import os
import hashlib

# BUF_SIZE is totally arbitrary, change for your app!
BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
SIZE = 1048576

users = {}

context = zmq.Context()
s = context.socket(zmq.REP)
s.bind('tcp://*:8001')

params = {}
while True:
    data = s.recv_string()
    print('*******')
    print(type(data))
    params = json.loads(data)
    if(params.get('opcion')=='upload'):
        s.send_string('ok')
        with open(params.get('file'), 'ab') as f:
            mbyte = s.recv_multipart()
            f.write(mbyte[0])
            s.send_string('ok')

    elif (params.get('opcion') == 'download'):
        username = params.get('user')
        counter=  open(params.get('file'), "rb")
        if users.get(username) == None:
            users[username] = 0
        position = users[username]
        counter.seek(position)
        byte = counter.read(SIZE)
        users[username] = counter.tell()
        s.send_multipart([byte])
        counter.close()

    elif (params.get('opcion') == 'list'):
        files = os.listdir('.')
        listFiles = '\n'.join(files)
        s.send_string(listFiles)

    elif (params.get('opcion') == 'sharelink'):
        md5 = hashlib.md5()
        with open(params.get('file'), "rb") as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)
            link = md5.hexdigest()
            s.send_string(link)

    elif (params.get('opcion') == 'downloadlink'):
        link = params.get('file')
        files = os.listdir('.')
        for file in files:
            md5 = hashlib.md5()
            with open(file, "rb") as f:
                while True:
                    data = f.read(BUF_SIZE)
                    if not data:
                        break
                    md5.update(data)
            print(md5.hexdigest())
            if link == md5.hexdigest():
                params['file'] = file
                data = json.dumps(params)
                s.send_string(data)
                break
            else:
                print('no encontro el archivo...')
    else:
        print('no existe operacion...')


    