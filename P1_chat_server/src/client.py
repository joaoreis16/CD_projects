"""CD Chat client program"""
import logging
import socket
import selectors
import sys
import fcntl
import os

from .protocol import CDProto, CDProtoBadFormat, TextMessage

logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)


class Client:
    """Chat Client process."""

    def __init__(self, name: str = "Foo"):
        """Initializes chat client."""

        self.username = name
        self.client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sel = selectors.DefaultSelector()
        self.CDP = CDProto()
        self.channel = [None]  # incialmente o cliente não está em nenhum server



    def connect(self):
        """Connect to chat server and setup stdin flags."""

        host = 'localhost'
        port = 8080
        self.client_sock.connect((host, port))
        
        self.sel.register(self.client_sock, selectors.EVENT_READ, self.recv_data)

        # enviar mensagem do tipo register
        register_type = self.CDP.register(self.username)
        self.CDP.send_msg(self.client_sock, register_type)



    # função para receber mensagens
    def recv_data(self, sock, mask):

        msg = self.CDP.recv_msg(self.client_sock)
        logging.debug('received "%s', msg)

        if type(msg) is TextMessage:
            print(msg.message)



    # function to be called when enter is pressed
    def got_keyboard_data(self, stdin, mask):

        data = stdin.read()
        data = data[:-1]    # retirar \\n do input lido

        if data != "":

            if data == "exit":      # caso o client queira sair
                self.client_sock.close()
                sys.exit(f">> bye {self.username}!")

            elif data.split()[0].strip() == "/join":       # mudar de cannal "/join [channel_name]"

                if len(data.split()) != 2:
                    print("[ERROR]: Channel name missing. Try again!")

                else:
                    self.channel.append(data.split()[1].strip())
                    
                    join_type = self.CDP.join(self.channel[-1])
                    self.CDP.send_msg(self.client_sock, join_type)

                    print(f'>> {self.username} has left and joined the server {self.channel[-1]}')

            else:
                mensagem = self.CDP.message(data, self.channel[-1]) 
                self.CDP.send_msg(self.client_sock, mensagem)
            


    def loop(self):
        """Loop indefinetely."""

        # set sys.stdin non-blocking
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

        self.sel.register(sys.stdin, selectors.EVENT_READ, self.got_keyboard_data)

        while True:
            sys.stdout.write(f'message| ')
            sys.stdout.flush()
            for k, mask in self.sel.select():
                callback = k.data
                callback(k.fileobj, mask)