import zmq
import json

def calcClient():
    print('Menu principal\n')
    num1 = int(input('escribe el primer numero: '))
    num2 = int(input('escribe el segundo numero: '))
    op = input('escribe la operacion a realizar (+ , - , *, / ): ')
    operacion = json.dumps({
        'num1':  num1,
        'num2':  num2,
        'op':  op,
    })

    return operacion
    

    

context = zmq.Context()

s = context.socket(zmq.REQ)
s.connect('tcp://localhost:8001')

datos = calcClient()

s.send_string(datos)
resultado = s.recv_string()
print(resultado)