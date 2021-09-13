import zmq

context = zmq.Context()

s = context.socket(zmq.REQ)
s.connect('tcp://localhost:8001')

s.send_string('hola')
m = s.recv_string()
print(m)