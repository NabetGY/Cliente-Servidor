import zmq
import sys
import json

SIZE = 1048576

params = {
    'user': sys.argv[1],
    'opcion': sys.argv[2],
    'file': sys.argv[3] if sys.argv[2] != 'list' else 0
}

print(params)
context = zmq.Context()
s = context.socket(zmq.REQ)
s.connect('tcp://localhost:8001')

if(params.get('opcion') == 'upload'):
    with open(params.get('file'), "rb") as f:
        Mbyte = f.read(SIZE)
        while True:
            if not Mbyte:
                break
            data = json.dumps(params)
            s.send_string(data)
            s.recv_string()
            s.send_multipart([Mbyte])
            s.recv_string()
            Mbyte = f.read(SIZE)
            
elif (params.get('opcion') == 'download'):
    with open(params.get('file'), 'ab') as f:
        while True:
            data = json.dumps(params)
            s.send_string(data)
            byte = s.recv_multipart()
            if len(byte[0])==0:
                break
            f.write(byte[0])

elif (params.get('opcion') == 'list'):
    data = json.dumps(params)
    s.send_string(data)
    listFiles = s.recv_string()
    print(listFiles)

elif (params.get('opcion') == 'sharelink'):
    data = json.dumps(params)
    s.send_string(data)
    link = s.recv_string()
    print(link)
    
elif (params.get('opcion') == 'downloadlink'):
    data = json.dumps(params)
    s.send_string(data)
    data = s.recv_string()
    params = json.loads(data)
    params['opcion'] = 'download'
    with open(params.get('file'), 'ab') as f:
        while True:
            data = json.dumps(params)
            s.send_string(data)
            byte = s.recv_multipart()
            if len(byte[0])==0:
                break
            f.write(byte[0])


    
# python cliente.py username upload file.ext