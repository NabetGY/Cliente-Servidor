import zmq

context = zmq.Context()

s = context.socket(zmq.REQ)
s.connect('tcp://localhost:8001')

with open("imagen.png", "rb") as f:
    byte = f.read()
    s.send_multipart([byte])
        
# python cliente.py username upload file.ext