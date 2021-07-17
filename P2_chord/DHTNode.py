""" Chord DHT node implementation. """
import socket
import threading
import logging
import pickle
import math
from utils import dht_hash, contains

class FingerTable:
    """Finger Table."""

    def __init__(self, node_id, node_addr, m_bits=10):
        """ Initialize Finger Table."""
        self.node_id = node_id
        self.node_addr = node_addr
        self.m_bits = m_bits
        
        self.finger = []
        for i in range(0 , m_bits):             # criar as entradas da finger table (nº entradas = m_bits)
            self.finger.append( (node_id, node_addr) ) 


    def fill(self, node_id, node_addr):
        """ Fill all entries of finger_table with node_id, node_addr."""
        for i in range(0, len(self.finger)):      
            self.finger[i] = (node_id, node_addr)       # preencher as entradas da finger table com os valores default

 
    def update(self, index, node_id, node_addr):
        """Update index of table with node_id and node_addr."""
        index = index - 1 
        self.finger[index] = (node_id, node_addr)       # alterar o valor na posição index da finger table


    def find(self, identification):
        """ Get node address of closest preceding node (in finger table) of identification. """
        for i in range(len(self.finger)):
            if contains(self.node_id, self.finger[i][0], identification): # caso contanha, retornar o endereço do nó anterior
                return self.finger[i - 1][1]

        return self.finger[len(self.finger)-1][1] # retornar o último nó da finger table ==> caso nenhum nó esteja contido 
        

    def refresh(self):
        """ Retrieve finger table entries."""
        lst = []
        for i in range(len(self.finger)):
            id_num = (self.node_id + pow(2, i)) % pow(2,self.m_bits) # id = (n + 2^(k−1)) mod 2^m
            lst.append( (i + 1, id_num, self.finger[i][1]) )

        return lst

        
    def getIdxFromId(self, id):
        for i in range(self.m_bits):
            index = (self.node_id + pow(2, i))      # index = aos nós que são procurados
            
            if (index > pow(2,self.m_bits) - 1):    # caso o node a procurar seja maior do que 1023, calculamo-lo de maneira circular
                index =  index - pow(2,self.m_bits)
               
            if index == id:
                return i + 1

        return None

    def __repr__(self):
        string = ""
        for f in self.finger:
            string += f'{str(f[0])}, '
        return string

    @property
    def as_list(self):
        """return the finger table as a list of tuples: (identifier, (host, port)).
        NOTE: list index 0 corresponds to finger_table index 1
        """ 
        return [f for f in self.finger]
        

class DHTNode(threading.Thread):
    """ DHT Node Agent. """

    def __init__(self, address, dht_address=None, timeout=3):
        """Constructor

        Parameters:
            address: self's address
            dht_address: address of a node in the DHT
            timeout: impacts how often stabilize algorithm is carried out
        """
        threading.Thread.__init__(self)
        self.done = False
        self.identification = dht_hash(address.__str__())
        self.addr = address  # My address
        self.dht_address = dht_address  # Address of the initial Node
        if dht_address is None:
            self.inside_dht = True
            # I'm my own successor
            self.successor_id = self.identification
            self.successor_addr = address
            self.predecessor_id = None
            self.predecessor_addr = None
        else:
            self.inside_dht = False
            self.successor_id = None
            self.successor_addr = None
            self.predecessor_id = None
            self.predecessor_addr = None

        #TODO create finger_table
        self.finger_table = FingerTable(self.identification, self.addr)

        self.keystore = {}  # Where all data is stored
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)
        self.logger = logging.getLogger("Node {}".format(self.identification))

    def send(self, address, msg):
        """ Send msg to address. """
        payload = pickle.dumps(msg)
        self.socket.sendto(payload, address)

    def recv(self):
        """ Retrieve msg payload and from address."""
        try:
            payload, addr = self.socket.recvfrom(1024)
        except socket.timeout:
            return None, None

        if len(payload) == 0:
            return None, addr
        return payload, addr

    def node_join(self, args):
        """Process JOIN_REQ message.

        Parameters:
            args (dict): addr and id of the node trying to join
        """ 

        self.logger.debug("Node join: %s", args)
        addr = args["addr"]
        identification = args["id"]
        if self.identification == self.successor_id:  # I'm the only node in the DHT
            self.successor_id = identification
            self.successor_addr = addr
            #TODO update finger table
            finger_lst = self.finger_table.as_list

            for i in range(len(finger_lst)):
                node = self.identification + pow(2, i)
                if node > pow(2,len(finger_lst)) - 1:
                    node = node - pow(2,len(finger_lst))

                if self.identification ==  finger_lst[i][0] or (contains(self.identification, self.successor_id, node) and self.successor_id <  finger_lst[i][0]):
                    self.finger_table.update( i+1 , self.successor_id, self.successor_addr) # se o nosso id for igual ao da posição i na finger list OU
                                                        # (se o node que cálculamos estiver entre o nosso node id e o seu sucessor E o sucessor for menor do que 
                                                                            # o valor do node na posição i na finger list)
            args = {"successor_id": self.identification, "successor_addr": self.addr}
            self.send(addr, {"method": "JOIN_REP", "args": args})
        elif contains(self.identification, self.successor_id, identification):
            args = {
                "successor_id": self.successor_id,
                "successor_addr": self.successor_addr,
            }
            self.successor_id = identification
            self.successor_addr = addr
            #TODO update finger table
            finger_lst = self.finger_table.as_list

            for i in range(len(finger_lst)):
                node = self.identification + pow(2, i)
                if node > pow(2,len(finger_lst)) - 1:
                    node = node - pow(2,len(finger_lst))

                if self.identification ==  finger_lst[i][0] or (contains(self.identification, self.successor_id, node) and self.successor_id <  finger_lst[i][0]):
                    self.finger_table.update( i+1 , self.successor_id, self.successor_addr)

            self.send(addr, {"method": "JOIN_REP", "args": args})
        else:
            self.logger.debug("Find Successor(%d)", args["id"])
            self.send(self.successor_addr, {"method": "JOIN_REQ", "args": args})
        self.logger.info(self)


    def get_successor(self, args):
        """Process SUCCESSOR message.

        Parameters:
            args (dict): addr and id of the node asking
        """

        self.logger.debug("Get successor: %s", args)
        #TODO Implement processing of SUCCESSOR message
        id_num = args["id"]
        addr = args["from"]

        # ask node n to find the successor of id
        if self.predecessor_id is None or contains( self.predecessor_id, self.identification, id_num): # se o predecessor for nulo ou o id estiver entre o predecessor e o identification, então enviamos para o identification
            dic = {"method": "SUCCESSOR_REP", "args": {"req_id": id_num, "successor_id": self.identification, "successor_addr": self.addr}}
            self.send(addr, dic)

        elif contains(self.identification, self.successor_id, id_num): # se o id estiver entre o identification e o sucessor, então enviamos para o identification
            dic = {"method": "SUCCESSOR_REP", "args": {"req_id": id_num, "successor_id": self.successor_id, "successor_addr": self.successor_addr}}
            self.send(addr, dic)

        else:
            dic = {"method": "SUCCESSOR", 'args': {"id": id_num, "from": addr}}
            self.send(self.successor_addr, dic)
            
                
    def notify(self, args):
        """Process NOTIFY message.
            Updates predecessor pointers.

        Parameters:
            args (dict): id and addr of the predecessor node
        """

        self.logger.debug("Notify: %s", args)
        if self.predecessor_id is None or contains(
            self.predecessor_id, self.identification, args["predecessor_id"]
        ):
            self.predecessor_id = args["predecessor_id"]
            self.predecessor_addr = args["predecessor_addr"]
        self.logger.info(self)

    def stabilize(self, from_id, addr):
        """Process STABILIZE protocol.
            Updates all successor pointers.

        Parameters:
            from_id: id of the predecessor of node with address addr
            addr: address of the node sending stabilize message
        """

        self.logger.debug("Stabilize: %s %s", from_id, addr)
        if from_id is not None and contains(
            self.identification, self.successor_id, from_id
        ):
            # Update our successor
            self.successor_id = from_id
            self.successor_addr = addr
            #TODO update finger table
            finger_lst = self.finger_table.as_list

            for i in range(len(finger_lst)):
                node = self.identification + pow(2, i)
                if node > pow(2,len(finger_lst)) - 1:      
                    node = node - pow(2,len(finger_lst))

                if self.identification ==  finger_lst[i][0] or (contains(self.identification, self.successor_id, node) and self.successor_id <  finger_lst[i][0]):
                    self.finger_table.update( i+1 , self.successor_id, self.successor_addr)

        # notify successor of our existence, so it can update its predecessor record
        args = {"predecessor_id": self.identification, "predecessor_addr": self.addr}
        self.send(self.successor_addr, {"method": "NOTIFY", "args": args})

        # TODO refresh finger_table
        refresh_lst = self.finger_table.refresh()
        for x in refresh_lst:
            args = {"id": x[1], "from": self.addr}
            self.get_successor(args)
        

    def put(self, key, value, address):
        """Store value in DHT.

        Parameters:
        key: key of the data
        value: data to be stored
        address: address where to send ack/nack
        """
        key_hash = dht_hash(key) 
        self.logger.debug("Put: %s %s", key, key_hash)

        #TODO Replace next code:
        # key_hash = nó atual
        if contains(self.identification, self.successor_id, key_hash):      # se o nó atual estiver entre o id e o sucessor
            dic = {"method": "PUT", "args": {"key": key, "value": value, "from": address}}
            self.send(self.successor_addr, dic)

        elif contains(self.predecessor_id, self.identification, key_hash): # se estiver dentro, então adicionamos ao dicionário keystore
            if key in self.keystore:
                dic = {'method': 'NACK'}
                self.send(address, dic)
            else:
                self.keystore[key] = value
                dic = {'method': 'ACK'}
                self.send(address, dic)

        else: # se não estiver, enviar mensagem para o nó sucessor
            addr = self.finger_table.find(key_hash) # buscar o endereço do nó que corresponde a esta key
            dic = {"method": "PUT", "args": {"key": key, "value": value, "from": address}}
            self.send(addr, dic) 
        


    def get(self, key, address):
        """Retrieve value from DHT.

        Parameters:
        key: key of the data
        address: address where to send ack/nack
        """
        key_hash = dht_hash(key)
        self.logger.debug("Get: %s %s", key, key_hash)

        #TODO Replace next code:
        if contains(self.identification, self.successor_id, key_hash):  # se o nó atual estiver entre o id e o sucessor
            dic = {"method": "GET", "args": {"key": key, "from": address}}
            self.send(self.successor_addr, dic)

        elif contains(self.predecessor_id, self.identification, key_hash): # se estiver entre o predecessor e o id, então adicionamos ao dicionário keystore
            value = self.keystore[key]
            dic = {'method': 'ACK', "args": value}
            self.send(address, dic)
            
        else: # se não estiver, enviar mensagem para o nó sucessor
            addr = self.finger_table.find(key_hash) # buscar o endereço do nó que corresponde a esta key
            dic = {"method": "GET", "args": {"key": key, "from": address}}
            self.send(addr, dic)


    def run(self):
        self.socket.bind(self.addr)
        
        # Loop untiln joining the DHT
        while not self.inside_dht:
            join_msg = {
                "method": "JOIN_REQ",
                "args": {"addr": self.addr, "id": self.identification},
            }
            self.send(self.dht_address, join_msg)
            payload, addr = self.recv()
            
            if payload is not None:
                
                output = pickle.loads(payload)
                self.logger.debug("O: %s", output)
                if output["method"] == "JOIN_REP":
                    args = output["args"]
                    self.successor_id = args["successor_id"]
                    self.successor_addr = args["successor_addr"]
                    #TODO fill finger table
                    self.finger_table.fill(self.successor_id, self.successor_addr)

                    self.inside_dht = True
                    self.logger.info(self)

        while not self.done:
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.info("O: %s", output)
                if output["method"] == "JOIN_REQ":
                    self.node_join(output["args"])
                elif output["method"] == "NOTIFY":
                    self.notify(output["args"])
                elif output["method"] == "PUT":
                    self.put(
                        output["args"]["key"],
                        output["args"]["value"],
                        output["args"].get("from", addr),
                    )
                elif output["method"] == "GET":
                    self.get(output["args"]["key"], output["args"].get("from", addr))
                elif output["method"] == "PREDECESSOR":
                    # Reply with predecessor id
                    self.send(
                        addr, {"method": "STABILIZE", "args": self.predecessor_id}
                    )
                elif output["method"] == "SUCCESSOR":
                    self.get_successor(output["args"])
                elif output["method"] == "STABILIZE":
                    # Initiate stabilize protocol
                    self.stabilize(output["args"], addr)
                elif output["method"] == "SUCCESSOR_REP":
                    #TODO Implement processing of SUCCESSOR_REP
                    index = output["args"]["req_id"]
                    succ_id = output["args"]["successor_id"]
                    succ_addr = output["args"]["successor_addr"]

                    index = self.finger_table.getIdxFromId(index)
                    if (index != None):
                        self.finger_table.update(index, succ_id, succ_addr)

            else:  # timeout occurred, lets run the stabilize algorithm
                # Ask successor for predecessor, to start the stabilize process
                self.send(self.successor_addr, {"method": "PREDECESSOR"})

    def __str__(self):
        return "Node ID: {}; DHT: {}; Successor: {}; Predecessor: {}; FingerTable: {}".format(
            self.identification,
            self.inside_dht,
            self.successor_id,
            self.predecessor_id,
            self.finger_table,
        )

    def __repr__(self):
        return self.__str__()
