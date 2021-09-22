import zmq
import json
def calcServer(datos):
    operacion = json.loads(datos)
    if operacion['op'] == '+':
        return str(operacion['num1'] +  operacion['num2'])
    elif operacion['op'] == '-':
        return str(operacion['num1'] -  operacion['num2'])
    elif operacion['op'] == '*':
        return str(operacion['num1'] *  operacion['num2'])
    elif operacion['op'] == '/':
        return str(operacion['num1'] /  operacion['num2'])
    else:
        return 'Operacion solicitada no valida...'


context = zmq.Context()

s = context.socket(zmq.REP)
s.bind('tcp://*:8001')

datos = s.recv_string()

s.send_string('mundo...')
