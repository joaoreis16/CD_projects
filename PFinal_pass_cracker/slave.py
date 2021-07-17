import time, socket, struct, json
from itertools import product
from base64 import b64encode
from string import ascii_letters, digits 
from protocol import Protocol

from server.const import (
    BANNED_TIME,
    COOLDOWN_TIME,
    NEW_PENALTY,
    MIN_VALIDATE,
    MAX_VALIDATE,
    MIN_TRIES,
    MAX_TRIES,
    PASSWORD_SIZE,
)

class Slave:
    def __init__(self) -> None:
        '''initialize slave'''
        self.list_slaves = []

        self.host = '172.17.0.2'
        self.port = 8000
        
        self.sock_main = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock_main.connect( (self.host, self.port) )
        print(">> Conectado com o main com sucesso")

        self.MCAST_GRP = '224.1.1.1'
        self.MCAST_PORT = 5007
        
        self.my_address = socket.gethostbyname(socket.gethostname())
        print(">> IP:", self.my_address)
        self.list_slaves.append( self.my_address )

        self.send_to_multicast(self.my_address)

        # recv multicast
        IS_ALL_GROUPS = True

        self.sock_recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock_recv.settimeout(0)
        self.sock_recv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if IS_ALL_GROUPS:
            # on this port, receives ALL multicast groups
            self.sock_recv.bind(('', self.MCAST_PORT))
        else:
            # on this port, listen ONLY to MCAST_GRP
            self.sock_recv.bind((self.MCAST_GRP, self.MCAST_PORT))
        mreq = struct.pack("4sl", socket.inet_aton(self.MCAST_GRP), socket.INADDR_ANY)

        self.sock_recv.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)


        # a sua própra socket
        self.own_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        print("LISTA DE SLAVES", self.list_slaves)
        self.work2do = self.divide(3)



    def send_to_multicast(self, addr):
        
        MULTICAST_TTL = 2

        sock_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock_send.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)

        join_msg = Protocol.join(addr)
        sock_send.sendto( join_msg, (self.MCAST_GRP, self.MCAST_PORT) )
        print(">> Message join enviada por multicast")


    def recv_multicast(self):
        try:
            dic = json.loads(self.sock_recv.recv(1024).decode("utf-8"))
            print(">> SLAVE ENCONTRADO [IP]:", dic["address"])
            if dic["command"] == "join":
                ip = dic["address"]
                if ip not in self.list_slaves:
                    self.list_slaves.append(ip)

            elif dic["command"] == "distribute":
                pass

        except socket.error:
            print(">> Nenhum slave encontrado")



    def get_protocol(self, username, userpass):
        msg = f"GET / HTTP/1.1\nHost: {self.host} : {self.port}\n"
        cod = f'{username}:{userpass}'
        header = b64encode( str.encode(cod) ).decode("ascii")
        msg += f'Authorization: Basic {header}\n\n'
        return msg


    def try_pass(self, msg):
        protocol = msg.encode("utf-8")
        self.sock_main.send(protocol)


    def getAllPasswords(self) -> list:
        lst = []    # lista com todas as passwords possiveis
        for i in product(ascii_letters + digits, repeat=PASSWORD_SIZE):
            lst.append(''.join(i))

        return lst


    def get_work(self):
        if len(self.work2do) != 0:
            work = self.work2do[0]
            self.work2do.pop(0)
            return work

        return None


    def divide(self, num):
        lst = self.getAllPasswords()
        tamanho = len(lst)
        avg = tamanho / float(num)
        out = []
        last = 0.0

        while last < tamanho:
            out.append(lst[int(last):int(last + avg)])
            last += avg

        return out


    def run(self):
        username = "root"

        # lst = self.get_work()
        lst = self.getAllPasswords()

        tries = 0
        for userpass in lst:
            tries += 1

            # evitar o estado banned
            if tries == MAX_TRIES - MIN_TRIES:

                # a cada CoolDownTime aproveitamos para mensagens de join, para manter controlo de quem está ativo
                self.recv_multicast()

                self.send_to_multicast(self.my_address) 
                print("LISTA DE SLAVES", self.list_slaves)

                time.sleep( (COOLDOWN_TIME/1000)%60 )
                tries = 0

            protocol = self.get_protocol(username, userpass)
            self.try_pass(protocol)
            print("Password:", userpass)

            response = self.sock_main.recv(1024).decode("utf-8") 
            if "OK" in response: 
                print(">> Password correta")
                break

            elif response[-1] == '\n':
                more_info = self.sock_main.recv(1024).decode('utf-8')
                response += more_info


if __name__ == "__main__":
    s = Slave()
    s.run()
