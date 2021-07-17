# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
import time

# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None

# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.index = -1

    def select_server(self):
        self.index += 1
        if self.index >= len(self.servers):
            self.index = 0
        
        return self.servers[self.index]
            
    def update(self, *arg):
        pass


# least connections policy
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        self.ligacoes = {}      # dic to save the connections of servers  { server : numClients }
        for s in self.servers:
            self.ligacoes[s] = 0

    def select_server(self):
        value = min(self.ligacoes.values())
        for server, clients in self.ligacoes.items():
            if clients == value:
                return server
        
        return None

    def update(self, *arg):
        delete = arg[0][0]
        conn = arg[0][1]

        if not delete:
            self.ligacoes[conn] += 1

        else: 
            self.ligacoes[conn] -= 1


# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        self.server_times = {}      # server : time
        self.init = {}
        for server in self.servers:
            self.server_times[server] = [0, 0]
            self.init[server] = [0.0000000000000000001]

    def select_server(self):
        lst = []
        for values in self.server_times.values():
            lst.append( values[0] )

        least_response_time = min(lst)

        for key in self.server_times.keys():
            if self.server_times[key][0] == least_response_time:
                return key
         
        return None

    def update(self, *arg):
        delete = arg[0][0]
        conn = arg[0][1]

        if not delete:
            self.init[conn].append( time.process_time() )

        else:
            t2 = time.process_time()

            if len(self.init[conn]) == 0:
                self.init[conn] = [0.0000000000000000001]

            diff_time = t2 - self.init[conn][0]
            self.init[conn].pop(0)

            if self.server_times[conn][1] == 0:
                self.server_times[conn] = [diff_time, 1]
            else:
                avg_time = (self.server_times[conn][0] * self.server_times[conn][1] + diff_time) / (self.server_times[conn][1] + 1)
                count = self.server_times[conn][1]
                self.server_times[conn] = [avg_time, count + 1]


POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}
        self.map_conn = {}          # client : server

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] =  upstream_sock

        self.map_conn[upstream_sock] = upstream_server
        self.map_conn[client_sock] = upstream_server

        self.policy.update( (False, upstream_server) )

    def delete(self, sock):
        server = self.map_conn[sock]
        self.policy.update( (True, server) ) 

        sel.unregister(sock)
        sock.close()

        if sock in self.map:
            self.map.pop(sock)

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    mapper.add(client, policy.select_server())

def read(conn, mask):
    data = conn.recv(4096)

    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)

    else:
        data_str = data.decode("utf-8") 
        data_store = data_str.split(" ")
        
        if "GET" in data_str:
            precision = data_store[1].split("/")[1]
            if precision in cache.keys():       # enviar diretamente para o cliente
                conn.send(cache[precision])
                
            else:
                mapper.get_sock(conn).send(data)

        elif "Content-Type:" in data_str:
            for i in range(len(data_store)):
                if data_store[i] == "precision":
                    precision = data_store[i+1]

            if len(cache) == 5:     # caso a cache estiver cheia, eliminamos a primeira entrada do dic cache, para ser adicionada uma nova
                remove_key = list(cache.keys())[0]
                cache.pop(remove_key)

            cache[precision] = data


def main(addr, servers, policy_class):
    global policy
    global mapper
    global cache

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)
    cache = {}

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
            
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()

    servers = [('127.0.0.1', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])
