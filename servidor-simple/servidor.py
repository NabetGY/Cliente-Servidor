import zmq

context = zmq.Context()

s = context.socket(zmq.REP)
s.bind('tcp://*:8001')

m = s.recv_string()
s.send_string('mundo...')
