"""CD Chat server program."""
import logging
import selectors
import socket

from .protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename="server.log", level=logging.DEBUG)

class Server:
    """Chat Server process."""

    def __init__(self):
  
        self.sel = selectors.DefaultSelector()
        self.sock_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        host = 'localhost'
        port = 8080
        self.sock_server.bind((host, port))
        print("--- Server Opened ---")

        self.sock_server.listen(100)

        self.sel.register(self.sock_server, selectors.EVENT_READ, self.accept)

        self.total_clients = {}     # socket : [user, [list_of_channels]] 

        

    def accept(self, sock, mask):
        sock_client, addr = sock.accept()
        print('Connected to ', addr)
        sock_client.setblocking(False)

        self.sel.register(sock_client, selectors.EVENT_READ, self.read)
    

       
    def read(self, conn, mask):

        msg = CDProto.recv_msg(conn)

        if msg: 
            if msg.command == "register":
                print(f'>> {msg.user} joined the server')
                self.total_clients[conn] = [msg.user, [None]]

            elif msg.command == "join":
                channel = msg.channel

                if channel in  self.total_clients[conn][1]: # caso o cliente volte a entrar num canal, é eliminado da lista de channels, para evitar repetições
                    self.total_clients[conn][1].remove(channel)

                self.total_clients[conn][1].append(channel)
                print(f'>> { self.total_clients[conn][0]} joined the channel {channel}')

            elif msg.command == "message":

                channel = msg.channel
                broadcast_msg = f'<<{channel}>> [{self.total_clients[conn][0]}]: {msg.message}' # mensagem para enviar para todos os clientes, irá sempre mostrar o canal de onde veio essa mensagem <<[channel_name]>>

                msg = CDProto.message(broadcast_msg, channel)

                print(f'{broadcast_msg} | channels: {self.total_clients[conn][1]}')

                self.send_broadcast_msg(conn, mask, channel, msg) # enviar a mensagem para todos os clientes
            
            logging.debug('received "%s', msg)
                
        else:
            print(f'>> {self.total_clients[conn][0]} has left the server')

            self.total_clients.pop(conn)
            self.sel.unregister(conn)
            
            conn.close()


    def send_broadcast_msg(self, conn, mask, channel, msg):
        '''Função para enviar as TextMessages para todos os clientes'''

        for sock in self.total_clients.keys(): 
            if sock != conn and channel in self.total_clients[sock][1]:     # a ultima posicao do vetor dos channels corresponde ao channel em que o client se encontra atualmente
                CDProto.send_msg(sock, msg)                                 # irá imprimir a mensagem se esta pertecer a um canal a que o cliente perteça



    def loop(self):
        """Loop indefinetely."""

        while True:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)