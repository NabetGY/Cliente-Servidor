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




def upload(params, ranges, servers):

    fileName = params.get( 'fileName' )
    indexName = fileName.split('.')[0]
    indexName = indexName+'.index'

    with open( fileName , 'rb' ) as file: 

        with open( indexName, 'a' ) as index:

            index.write( fileName+'\n' )
            mbyte = file.read(SIZE)

            while True:
                
                if (not len(mbyte)):
                    break
                hashMb = megaToSha( mbyte )

                for range in ranges:

                    if range.member(hashMb):

                        index.write( str(hashMb)+'\n' )
                        data = json.dumps(params)
                        socket = servers.get(range.lb)

                        socket.send_string(data)
                        socket.recv_string()

                        socket.send_multipart([mbyte])
                        socket.recv_string()

                        mbyte = file.read(SIZE)
                        break 

def download(params, ranges, servers ):


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
                for range in ranges:
    
                    if range.member(hashMb):

                        socket = servers.get(range.lb)

                        params['fileName'] = hashMb

                        data = json.dumps(params)

                        socket.send_string(data)
                        byte = socket.recv_multipart()
        
                        file.write(byte[0])

                        break
    
    



def main():
    
    params = {
        'user': sys.argv[1],
        'opcion': sys.argv[2],
        'fileName': sys.argv[3] if sys.argv[2] != 'list' else 0
    }

    serverNames = ['serv1', 'serv2', 'serv3', 'serv4', 'serv5']

    servers = []

    for num in range(1,6):
        context = zmq.Context()
        socket = context.socket( zmq.REQ )
        socket.connect( 'tcp://127.0.0.{}:800{}'.format(num, num) )
        servers.append( socket )


    serverSha = []
    serverSocket ={}

    for num in range(5):
        sha_one = strToSha( serverNames[num] )
        serverSha.append( sha_one )
        serverSocket[sha_one] = servers[num]


    serverSha.sort()
    ranges = []
    
    for n in range( len(serverSha)-1 ):
        lb = serverSha[n]
        ub = serverSha[n+1]
        ranges.append( Range( lb,ub ) )
    ranges.append( Range( serverSha[4], serverSha[0] ) )


    if(params.get('opcion') == 'upload'):
        upload(params, ranges, serverSocket)
    elif (params.get('opcion') == 'download'):
        download(params, ranges, serverSocket)



if __name__ == '__main__':
    main()
    