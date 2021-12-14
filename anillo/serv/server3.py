import zmq
import sys
import json
import hashlib
import os

#############################################################################################3

# BUF_SIZE is totally arbitrary, change for your app!
BUF_SIZE = 65536   # lets read stuff in 64kb chunks!
SIZE = 512


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
            return "[" + str( self.lb ) + " , 2**160) U [" + "0 , " + str(
                self.ub
            ) + ")"
        else:
            return "[" + str( self.lb ) + " , " + str( self.ub ) + ")"


###########################################################################################################################################


def status( rango, predecesor, sucesor ):
    print( "mi nuevo rango :" + rango.toStr() )
    print( "sucesor :" + sucesor )
    print( "predecesor :" + predecesor )
    print( "\n" )


######################################################################################################################


def megaToSha( megabyte ):
    hash_object = hashlib.sha1( megabyte )
    name = hash_object.hexdigest()
    nameAsNum = int( name, 16 )
    return nameAsNum


####################################################################################################################3


def sha1Serv( serverName ):
    hash_object = hashlib.sha1( serverName.encode() )
    name = hash_object.hexdigest()
    nameAsNum = int( name, 16 )
    return nameAsNum


class Server:

    def __init__( self, ipConnection, ConnectionType ):
        self.ip = ''
        self.setIP()
        self.id = str( sha1Serv( str( self.ip ) ) )
        self.socket = None
        self.successor = ''
        self.predecessor = ''
        self.range = None
        self.bootstrap_or_connect( ConnectionType, ipConnection )

    def setIP( self ):
        self.ip = (
            os.popen(
                "ip addr show ztc3q6fy2x | grep \"\<inet\>\" | awk '{ print $2 }' | awk -F \"/\" '{ print $1 }'"
            ).read().strip()
        )

    def bootstrap_or_connect( self, ConnectionType, ipConnection ):
        if ConnectionType == '--bootstrap':
            self.bootstrap()
        else:
            self.connect( ipConnection )

    def bootstrap( self ):

        self.successor = self.ip
        self.predecessor = self.ip
        self.range = Range( 0, pow( 2, 160 ) )
        context = zmq.Context()
        self.socket = context.socket( zmq.REP )
        self.socket.bind( "tcp://" + self.ip + ":8001" )
        print( "Servidor inicial..." )
        print( "Mi rango es: " + self.range.toStr() )
        print( "\n" )

    def connect( self, ipConnection ):
        context = zmq.Context()

        cliente = context.socket( zmq.REQ )
        cliente.connect( "tcp://" + ipConnection )
        print( "Soy nuevo y trato de ubicarme!..." )
        print( "\n" )

        info = {
            "opcion": "ubicame!",
            "idServer": self.id,
            "ipServer": self.ip,
        }

        infoJSON = json.dumps( info )

        while True:

            cliente.send_json( infoJSON )
            data = cliente.recv_json()
            opciones = json.loads( data )

            if opciones.get( "opcion" ) == "loUbico":

                self.successor = opciones.get( "sucesor" )
                self.predecessor = opciones.get( "predecesor" )

                self.balance( cliente )

                cliente = context.socket( zmq.REQ )
                cliente.connect( "tcp://" + self.predecessor + ":8001" )

                newSucesor = {
                    "opcion": "newSucesor",
                    "sucesor": self.ip,
                }

                newSucesorJSON = json.dumps( newSucesor )
                cliente.send_json( newSucesorJSON )

                idSucesor = cliente.recv_string()
                self.range = Range( int( idSucesor ), int( self.id ) )

                cliente.close()

                self.socket = context.socket( zmq.REP )
                self.socket.bind( "tcp://" + self.ip + ":8001" )

                status( self.range, self.predecessor, self.successor )
                break

            elif opciones.get( "opcion" ) == "noTeUbique":

                siguiente = opciones.get( "sucesor" )

                cliente = context.socket( zmq.REQ )
                cliente.connect( "tcp://" + siguiente + ":8001" )

    def balance( self, cliente ):

        data = {
            "opcion": "balance",
        }
        dataJSON = json.dumps( data )
        cliente.socket.send_json( dataJSON )
        while True:

            mbyte = cliente.recv_multipart()
            cliente.send_string( 'ok' )

            if mbyte[ 0 ] == False:
                return

            hashMb = megaToSha( mbyte[ 0 ] )
            fileName = str( hashMb )
            with open( fileName, "ab" ) as file:
                file.write( mbyte[ 0 ] )

    def menu( self ):

        while True:

            data = self.socket.recv_json()
            opciones = json.loads( data )

            if opciones.get( "opcion" ) == "ubicame!":

                idServer = opciones.get( "idServer" )

                pertenece = self.range.member( int( idServer ) )

                if pertenece:

                    serverData = {
                        "sucesor": self.successor,
                        "predecesor": self.predecessor,
                        "opcion": "loUbico",
                    }

                    data = json.dumps( serverData )
                    self.socket.send_json( data )

                    self.range = Range( int( idServer ), int( self.id ) )
                    self.predecessor = opciones.get( "ipServer" )
                    status( self.range, self.predecessor, self.successor )

                else:
                    infoSucesor = {
                        "sucesor": self.successor, "opcion": "noTeUbique"
                    }

                    data = json.dumps( infoSucesor )
                    self.socket.send_json( data )

            elif opciones.get( "opcion" ) == "newSucesor":

                self.successor = opciones.get( "sucesor" )
                self.socket.send_string( self.id )

                status( self.range, self.predecessor, self.successor )

            elif opciones.get( "opcion" ) == "upload":

                self.socket.send_string( "ok" )
                mbyte = self.socket.recv_multipart()

                hashMb = megaToSha( mbyte[ 0 ] )

                if self.range.member( hashMb ):

                    fileName = str( hashMb )
                    with open( fileName, "ab" ) as file:
                        file.write( mbyte[ 0 ] )
                        resp = { "opcion": "exito"}

                        data = json.dumps( resp )
                        self.socket.send_json( data )

                else:
                    infoSucesor = {
                        "sucesor": self.successor, "opcion": "noEsMio"
                    }

                    data = json.dumps( infoSucesor )
                    self.socket.send_json( data )

            elif opciones.get( "opcion" ) == "download":

                hashMb = opciones.get( "fileName" )

                archivo = str( hashMb )

                if self.range.member( hashMb ):
                    with open( archivo, "rb" ) as file:
                        mByte = file.read()

                        infoSucesor = { "opcion": "esMio"}

                        data = json.dumps( infoSucesor )
                        self.socket.send_json( data )

                        self.socket.recv_string()

                        self.socket.send_multipart( [ mByte ] )

                else:
                    infoSucesor = {
                        "sucesor": self.successor, "opcion": "noEsMio"
                    }

                    data = json.dumps( infoSucesor )
                    self.socket.send_json( data )

            elif opciones.get( "opcion" ) == "balance":

                files = os.listdir( '.' )

                for item in files:

                    hashMb = opciones.get( item )

                    archivo = str( hashMb )

                    if not self.range.member( hashMb ):
                        with open( archivo, "rb" ) as file:
                            mByte = file.read()

                            self.socket.send_multipart( [ mByte, True ] )
                            self.socket.recv_string()
                            os.remove( archivo )

                self.socket.send_multipart( [ False ] )


#############################################################################################


def main():

    ConnectionType = sys.argv[ 1 ]
    ipConnection = sys.argv[ 2 ] if sys.argv[ 1 ] != "--bootstrap" else None

    server = Server( ipConnection, ConnectionType )
    server.menu()


#############################################################################################3

if __name__ == "__main__":
    main()