import zmq
import sys
import json
import os
  
parameters = {
    'id': sys.argv[1],
    'opcion': sys.argv[2],
    'ip': sys.argv[3] if sys.argv[2] != '--bootstrap' else None
}

ipServer = os.popen('ip addr show ztc3q6fy2x | grep "\<inet\>" | awk \'{ print $2 }\' | awk -F "/" \'{ print $1 }\'').read().strip()


###########################################################################################################################################

def status( rango, predecesor, sucesor ):
    print('mi nuevo rango :'+rango.toStr())
    print('sucesor :'+sucesor)
    print('predecesor :'+predecesor)
    print('\n')


######################################################################################################################

def menu( server, rango, predecesor, sucesor ):

    while( True ):

        data = server.recv_json()
        opciones = json.loads(data)

        if ( opciones.get( 'opcion' ) == 'ubicame!' ):

            idServer = opciones.get( 'idServer' )

            pertenece = rango.member( int(idServer) )

            if ( pertenece ):

                serverData = {
                    'sucesor': ipServer,
                    'predecesor': predecesor,
                    'opcion' : 'loUbico'
                }
                
                data = json.dumps( serverData )
                server.send_json( data )
                
                rango = Range( int(idServer), int(parameters.get('id')) )
                predecesor = opciones.get('ipServer')
                status( rango, predecesor, sucesor )

            else:
                infoSucesor = {
                    'sucesor': sucesor,
                    'opcion' : 'noTeUbique'
                }

                data = json.dumps(infoSucesor)
                server.send_json(data)
        
        elif ( opciones.get( 'opcion' ) == 'newSucesor' ):
            
            sucesor = opciones.get( 'sucesor' )
            server.send_string( parameters.get('id') )
            
            status( rango, predecesor, sucesor )


#############################################################################################3

def bootstrap():
    context = zmq.Context()
    serverOne = context.socket(zmq.REP)
    miSucesor = ipServer
    miPredecesor = ipServer
    miRango = Range(0, 64)

    serverOne.bind('tcp://'+ipServer+':8001')
    print('Servidor inicial...')
    print('Mi rango es: '+miRango.toStr())
    print('\n')

    menu( serverOne, miRango, miPredecesor, miSucesor )


#############################################################################################3


def connect():
    context = zmq.Context()
    server = None
    miSucesor = None
    miPredecesor = None
    miRango = None

    cliente = context.socket(zmq.REQ)
    cliente.connect( 'tcp://'+parameters.get('ip') )
    print('Soy nuevo y trato de ubicarme!...')
    print('\n')
    
    info = {
        'opcion': 'ubicame!',
        'idServer': parameters.get('id'),
        'ipServer': ipServer,
    }

    infoJSON = json.dumps(info)

    while(True):

        cliente.send_json( infoJSON )
        data = cliente.recv_json()
        opciones = json.loads(data)

        if ( opciones.get( 'opcion' ) == 'loUbico' ):
            
            miSucesor = opciones.get( 'sucesor' )
            miPredecesor = opciones.get( 'predecesor' )
            
            cliente = context.socket(zmq.REQ)
            cliente.connect('tcp://'+miPredecesor+':8001')

            newSucesor = {
                'opcion' : 'newSucesor',
                'sucesor': ipServer,
            }

            newSucesorJSON = json.dumps(newSucesor)   
            cliente.send_json( newSucesorJSON )
            
            idSucesor = cliente.recv_string()
            miRango = Range( int(idSucesor), int(parameters.get('id')) )
            cliente.close()
            
            server = context.socket(zmq.REP)
            server.bind('tcp://'+ipServer+':8001')
            
            status( miRango, miPredecesor, miSucesor )
            break
        
        elif ( opciones.get( 'opcion' ) == 'noTeUbique' ):
            
            siguiente = opciones.get( 'sucesor' )
            
            cliente = context.socket(zmq.REQ)
            cliente.connect('tcp://'+siguiente+':8001')
    
    menu( server, miRango, miPredecesor, miSucesor )

#############################################################################################3

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
            return '[' + str(self.lb) + ' , 64) U [' + '0 , ' +  str(self.ub) + ')'
        else:
            return '[' + str (self.lb) + ' , ' + str(self.ub) + ')'


#############################################################################################3


def main():
    if ( parameters.get('opcion') == '--bootstrap' ):
        bootstrap()
    elif ( parameters.get('opcion') == '--connect' ):
        connect()

#############################################################################################3


if __name__ == '__main__':
    main()