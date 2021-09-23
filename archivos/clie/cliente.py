import zmq
import sys

user = sys.argv[1]
opcion = sys.argv[2]
if opcion != list:
    file = sys.argv[3]

context = zmq.Context()
s = context.socket(zmq.REQ)
s.connect('tcp://localhost:8001')

if(opcion == 'upload'):
    s.send_string(opcion)
    s.recv_string()
    s.send_string(user)
    s.recv_string()
    s.send_string(file)
    s.recv_string()
    with open(file, "rb") as f:
        byte = f.read()
        s.send_multipart([byte])
elif (opcion == 'download'):
    s.send_string(opcion)
    s.recv_string()
    s.send_string(user)
    s.recv_string()
    s.send_string(file)
    with open(file, 'wb') as f:
        byte = s.recv_multipart()
        f.write(byte[0])
elif (opcion == 'list'):
    s.send_string(opcion)
    s.recv_string()
    s.send_string(user)
    listFiles = s.recv_string()
    print(listFiles)
elif (opcion == 'sharelink'):
    s.send_string(opcion)
    s.recv_string()
    s.send_string(user)
    s.recv_string()
    s.send_string(file)
    link = s.recv_string()
    print(link)
elif (opcion == 'downloadlink'):
    s.send_string(opcion)
    s.recv_string()
    s.send_string(user)
    s.recv_string()
    s.send_string(file)
    nameFile = s.recv_string()
    s.send_string('ok')
    with open(nameFile, 'wb') as f:
        byte = s.recv_multipart()
        f.write(byte[0])


    
# python cliente.py username upload file.ext