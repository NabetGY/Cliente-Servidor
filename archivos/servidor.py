import zmq

context = zmq.Context()

s = context.socket(zmq.REP)
s.bind('tcp://*:8001')

with open('imagenCopia.png', 'wb') as f:
    byte = s.recv_multipart()
    f.write(byte[0])
        
print('Finalizado con cuye de exito...')
