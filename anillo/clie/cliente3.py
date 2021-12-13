import zmq
import sys
import json
import time
import hashlib

SIZE = 1048576


def strToSha( string ):
    hash_object = hashlib.sha1( string.encode() )
    name = hash_object.hexdigest()
    nameAsNum = int( name, 16 )
    return nameAsNum


def megaToSha( megabyte ):
    hash_object = hashlib.sha1( megabyte )
    name = hash_object.hexdigest()
    nameAsNum = int( name, 16 )
    return nameAsNum


class Range:

    def __init__( self, lb, ub ):
        self.lb = lb
        self.ub = ub

    def isFirst( self ):
        return self.lb > self.ub

    def member( self, id ):
        if self.isFirst():
            return ( id >= self.lb and
                     id < 1 << 160 ) or ( id >= 0 and id < self.ub )
        else:
            return id >= self.lb and id < self.ub

    def toStr( self ):
        if self.isFirst():
            return '[' + str( self.lb ) + ' , 2^160) U [' + '0 , ' + str(
                self.ub
            ) + ')'
        else:
            return '[' + str( self.lb ) + ' , ' + str( self.ub ) + ')'


'''                     CLASE CLIENTE               '''


class Client:

    def __init__( self, username, fileName, ip, opcion ):
        self.username = username
        self.opcion = opcion
        self.fileName = fileName
        self.ip = ip
        self.socket = None
        self.connection()

    def connection( self ):
        context = zmq.Context()
        self.socket = context.socket( zmq.REQ )
        self.socket.connect( 'tcp://' + self.ip )
        print( 'Soy un nuevo cliente!...' )

    def opcions( self ):
        if ( self.opcion == 'upload' ):
            self.upload()
        elif ( self.opcion == 'download' ):
            self.download()

    def upload( self ):

        indexName = self.fileName.split( '.' )[ 0 ]
        indexName = indexName + '.index'

        with open( self.fileName, 'rb' ) as file:

            with open( indexName, 'a' ) as index:

                index.write( self.fileName + '\n' )
                mbyte = file.read( SIZE )

                while True:

                    if ( not len( mbyte ) ):
                        break
                    hashMb = megaToSha( mbyte )

                    index.write( str( hashMb ) + '\n' )
                    params = {
                        'user': self.username,
                        'opcion': self.opcion,
                        'fileName': self.fileName,
                        'ip': self.ip
                    }
                    data = json.dumps( params )

                    while True:
                        self.socket.send_json( data )
                        self.socket.recv_string()

                        self.socket.send_multipart( [ mbyte ] )
                        respuestaJSON = self.socket.recv_json()
                        respuesta = json.loads( respuestaJSON )
                        print( respuesta )
                        if ( respuesta.get( 'opcion' ) == 'noEsMio' ):
                            print( 'no era de el...' )
                            siguiente = respuesta.get( 'sucesor' )
                            context = zmq.Context()
                            self.socket = context.socket( zmq.REQ )
                            self.socket.connect(
                                'tcp://' + siguiente + ':8001'
                            )

                        else:
                            print( 'si era de el...' )
                            break

                    mbyte = file.read( SIZE )

            print( 'termine de subir' )

    def download( self ):

        with open( self.fileName, 'r' ) as index:
            fileName2 = index.readline()

            fileName = fileName2.split( '\n' )[ 0 ]

            with open( '4' + fileName, 'ab' ) as file:
                while True:
                    hashMb = index.readline()
                    print( hashMb )
                    if ( len( hashMb ) == 0 ):
                        break

                    hashMb = int( hashMb )

                    params = {
                        'user': self.username,
                        'opcion': self.opcion,
                        'fileName': hashMb,
                        'ip': self.ip
                    }

                    data = json.dumps( params )
                    self.socket.send_json( data )

                    respuestaJSON = self.socket.recv_json()
                    respuesta = json.loads( respuestaJSON )

                    while ( respuesta.get( 'opcion' ) == 'noEsMio' ):

                        print( 'no era de el...' )
                        siguiente = respuesta.get( 'sucesor' )
                        context = zmq.Context()
                        self.socket = context.socket( zmq.REQ )
                        self.socket.connect( 'tcp://' + siguiente + ':8001' )

                        data = json.dumps( params )
                        self.socket.send_json( data )

                        respuestaJSON = self.socket.recv_json()
                        respuesta = json.loads( respuestaJSON )

                    self.socket.send_string( 'Envielo' )

                    byte = self.socket.recv_multipart()

                    file.write( byte[ 0 ] )


def main():

    user = sys.argv[ 1 ]
    opcion = sys.argv[ 2 ]
    fileName = sys.argv[ 3 ] if sys.argv[ 2 ] != 'list' else 0
    ip = sys.argv[ 4 ]

    client = Client( user, fileName, ip, opcion )
    client.opcions()

    print( 'Soy un nuevo cliente!...' )


if __name__ == '__main__':
    main()