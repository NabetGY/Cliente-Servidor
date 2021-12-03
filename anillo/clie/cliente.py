import zmq
import sys
import json
import time

from zmq.backend import has

SIZE = 1048576

import hashlib

def strToSha(string):
    hash_object = hashlib.sha1( string.encode() )
    name = hash_object.hexdigest()
    nameAsNum = int( name, 16 )
    return nameAsNum

def megaToSha(megabyte):
    hash_object = hashlib.sha1( megabyte )
    name = hash_object.hexdigest()
    nameAsNum = int( name, 16 )
    return nameAsNum

class Range:
    def __init__(self,lb,ub):
        self.lb = lb
        self.ub = ub
    
    def isFirst(self):
        return self.lb > self.ub
    
    def member(self, id):
        if self.isFirst():
            return (id >= self.lb and id < 1<<160) or (id >= 0 and id < self.ub )
        else:
            return id >= self.lb and id < self.ub
    
    def toStr(self):
        if self.isFirst():
            return '[' + str(self.lb) + ' , 2^160) U [' + '0 , ' +  str(self.ub) + ')'
        else:
            return '[' + str (self.lb) + ' , ' + str(self.ub) + ')'




def upload(params, server):

    fileName = params.get( 'fileName' )
    indexName = fileName.split('.')[0]
    indexName = indexName+'.index'

    with open( fileName , 'rb' ) as file: 

        with open( indexName, 'a' ) as index:

            index.write( fileName+'\n' )
            mbyte = file.read(SIZE)

            while True:
                
                if ( not len(mbyte) ):
                    break
                hashMb = megaToSha( mbyte )

                index.write( str(hashMb)+'\n' )
                data = json.dumps(params)
                print(data)

                
                while True:
                    server.send_json(data)
                    server.recv_string()
                    
                    
                    server.send_multipart([mbyte])
                    respuestaJSON = server.recv_json()
                    respuesta = json.loads(respuestaJSON)
                    print(respuesta)
                    if ( respuesta.get( 'opcion' ) == 'noEsMio' ):
                        print('no era de el...')
                        siguiente = respuesta.get( 'sucesor' )
                        context = zmq.Context()
                        server = context.socket(zmq.REQ)
                        server.connect('tcp://'+siguiente+':8001')
                    
                    else:
                        print('si era de el...')
                        break
    
                mbyte = file.read(SIZE)
                
        print('termine de subir') 

def download(params, server ):

    

    with open(params.get('fileName'), 'r' ) as index:
        fileName2  = index.readline()

        fileName =  fileName2.split('\n')[0]

        with open('4'+fileName, 'ab') as file:
            while True:
                hashMb = index.readline()
                print(hashMb)
                if ( len(hashMb)==0 ):
                    break
                
                hashMb = int(hashMb)
                
                params['fileName']=hashMb
                data = json.dumps(params)
                server.send_json(data)
                
                respuestaJSON = server.recv_json()
                respuesta = json.loads(respuestaJSON)
                
                while (respuesta.get('opcion') == 'noEsMio'):
                    
                    print('no era de el...')
                    siguiente = respuesta.get( 'sucesor' )
                    context = zmq.Context()
                    server = context.socket(zmq.REQ)
                    server.connect('tcp://'+siguiente+':8001')
                    
                    
                    data = json.dumps(params)
                    server.send_json(data)
                    
                    respuestaJSON = server.recv_json()
                    respuesta = json.loads(respuestaJSON)
                
                server.send_string('Envielo')
                
                byte = server.recv_multipart()
                
                file.write(byte[0])
    
    
    



def main():
    
    params = {
        'user': sys.argv[1],
        'opcion': sys.argv[2],
        'fileName': sys.argv[3] if sys.argv[2] != 'list' else 0,
        'ip': sys.argv[4]
    }
    
    context = zmq.Context()
    cliente = context.socket(zmq.REQ)
    cliente.connect( 'tcp://'+params.get('ip') )
    print('Soy un nuevo cliente!...')

    #serverNames = ['serv1', 'serv2', 'serv3', 'serv4', 'serv5']

    #servers = []
    

    if(params.get('opcion') == 'upload'):
        upload(params, cliente)
    elif (params.get('opcion') == 'download'):
        download(params, cliente)



if __name__ == '__main__':
    main()
    