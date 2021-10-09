import json
import zmq
import os
import hashlib
import sys

# BUF_SIZE is totally arbitrary, change for your app!
BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
SIZE = 512

def megaToSha(megabyte):
    hash_object = hashlib.sha1( megabyte )
    name = hash_object.hexdigest()
    nameAsNum = int( name, 16 )
    return nameAsNum


def main():
    users = {}
    params = {}

    numDir = sys.argv[1]
    port = 'tcp://127.0.0.{}:800{}'.format(numDir, numDir)

    context = zmq.Context()
    socket = context.socket( zmq.REP )
    socket.bind( port) 

    dir = './serv{}/'.format(numDir)
    print(dir)
    
    while True:

        data = socket.recv_string()

        params = json.loads(data)

        if(params.get('opcion')=='upload'):
            socket.send_string('ok')
            upload(socket, dir)
        elif (params.get('opcion') == 'download'):
            download(params, dir, socket)










def upload(socket, dir):
    
    mbyte = socket.recv_multipart()
    socket.send_string('ok')

    hashMb = megaToSha( mbyte[0] )
    fileName = dir+str( hashMb )

    with open( fileName, 'ab' ) as file:
        file.write( mbyte[0] )



def download(params, dir, socket):
        
        hashMb = params.get('fileName')
        print(params)
        print(hashMb)
        fileName = dir+str(hashMb)

        with open(fileName, 'rb') as file:
            mByte = file.read()
            socket.send_multipart([mByte])



if __name__ == '__main__':
    main()
    