import zmq
import sys
import json
import os

class Range:
    def __init__(self,lb,ub):
        self.lb = lb
        self.ub = ub
    
    def isFirst(self):
        print('lb= '+str(self.lb)+' ub='+str(self.ub))
        return self.lb > self.ub
    
    def member(self, id):
        if self.isFirst():
            return (id >= self.lb and id < 1<<160) or (id >= 0 and id < self.ub )
        else:
            return id >= self.lb and id < self.ub
    
    def toStr(self):
        print('is first :'+str(self.isFirst()))
        if self.isFirst():
            return '[' + str(self.lb) + ' , 64) U [' + '0 , ' +  str(self.ub) + ')'
        else:
            return '[' + str (self.lb) + ' , ' + str(self.ub) + ')'



parameters = {
    'id': sys.argv[1],
    'opcion': sys.argv[2],
    'ip': sys.argv[3] if sys.argv[2] != '--bootstrap' else None
}

ipServer = ipv4 = os.popen('ip addr show ztc3q6fy2x | grep "\<inet\>" | awk \'{ print $2 }\' | awk -F "/" \'{ print $1 }\'').read().strip()
rango = None
aux =None
context = zmq.Context()
server = context.socket(zmq.REP)
cliente = context.socket(zmq.REQ)
sucesor = None


if parameters.get('opcion')=='--bootstrap':
    sucesor = ipServer
    rango = Range(0, 64)
    
    server.bind('tcp://'+ipServer+':8001')
    print('yo soy ek primogenito')
    print('pertenece a mi rango: '+rango.toStr())

    while(True):
        id = server.recv_string()

        aux = rango.member( int(id) )

        if (aux):

            lista = [aux, ipServer, parameters.get('id')]
            data = json.dumps(lista)
            server.send_json(data)

            sucesor = server.recv_string()
            server.send_string('ok')

            rango = Range( int(id), int(parameters.get('id')) )

            print('rango :'+rango.toStr())
            print('sucesor :'+id+' con ip: '+sucesor)


        else:
            lista = [aux, sucesor]
            data = json.dumps(lista)
            server.send_json(data)

else:
    cliente = context.socket(zmq.REQ)
    print(parameters.get('ip'))
    cliente.connect('tcp://'+parameters.get('ip'))
    print('me concete con mero exito...')
    print(parameters.get('id'))
    cliente.send_string(parameters.get('id'))
    
    
    while(True):

        data = cliente.recv_json()
        lista = json.loads(data)

        if (lista[0]):

            sucesor = lista[1]
            id = lista[2]
            rango = Range(int(id), int(parameters.get('id')) )

            cliente.send_string( ipServer )
            cliente.recv_string()
            cliente.close()


            print('rango :'+rango.toStr())
            print('sucesor :'+id+' con ip: '+sucesor)

            server.bind('tcp://'+ipServer+':8001')
            aux = None
            break
        
        else:
            siguiente = cliente.recv_string()
            cliente.connect('tcp://'+siguiente+':8001')
            cliente.send_string(parameters.get('id'))


while(True):
    id = server.recv_string()
    aux = rango.member( int(id) )
    if (aux):
        server.send_string( str(aux))
        server.recv_string()

        server.send_string( ipServer )
        sucesor = server.recv_string()

        print('id del primo: '+parameters.get('id'))
        server.send_string( parameters.get('id') )
        server.recv_string()

        rango = Range( int(id), int(parameters.get('id')) )

        print('rango :'+rango.toStr())
        print('sucesor :'+id+' con ip: '+sucesor)


    else:
        server.send_string( str(aux))
        server.recv_string()
        server.send_string(sucesor)



