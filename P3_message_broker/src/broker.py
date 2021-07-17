"""Message Broker"""
import enum
from queue import Empty
from typing import Dict, List, Any, Tuple
import socket, selectors
import json, pickle
import xml.etree.ElementTree as XMLTree

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2

class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        print("Listen", self._host, self._port)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sel = selectors.DefaultSelector()

        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.sock.bind( (self._host, self._port) )
        self.sock.listen(100)

        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)

        # dictionaries to store info
        self.topic_dic = {}     # topic : last message 
        self.topic_subs = {}    # topic : List< (socket, serializer) >
        self.total_sock = {}    # socket : serializer


    def accept(self, sock, mask):
        conn, addr = sock.accept()
        self.sel.register(conn, selectors.EVENT_READ, self.read)


    def read(self, conn, mask):
        header = int.from_bytes(conn.recv(2) , "big")
        if header == 0:
            for topic in self.topic_subs: 
                self.unsubscribe(topic, conn) 
            self.sel.unregister(conn)
            conn.close()
            return

        msg = conn.recv(header)
        if conn not in list(self.total_sock.keys()):
            # caso for a primeira mensagem, fazer decode em pickle
            dic_msg = self.msg_converter(msg, Serializer.PICKLE)
            
        else:
            dic_msg = self.msg_converter(msg, self.total_sock[conn])
        

        if dic_msg:

            if dic_msg["method"] == "ack":
                _format = dic_msg["format"]

                if _format == "json":
                    self.total_sock[conn] = Serializer.JSON
                    print(">> nova socket: json")

                elif _format == "xml":
                    self.total_sock[conn] = Serializer.XML
                    print(">> nova socket: xml")
                
                elif _format == "pickle_ric":
                    self.total_sock[conn] = Serializer.PICKLE
                    print(">> nova socket: pickle")


            elif dic_msg["method"] == "subscribe":
                topic = dic_msg["topic"]
                _format = self.total_sock[conn]

                self.subscribe(topic, conn, _format)
                print(">> subscrição no tópico", topic)


            elif dic_msg["method"] == "publish":
                topic = dic_msg["topic"]
                msg = dic_msg["message"]

                self.topic_dic[topic] = msg
                print(">> publicação no tópico", topic,"com mensagem", msg)
                
                for t in list(self.topic_subs.keys()):
                    if t in topic:
                        for conn in self.topic_subs[t]:
                            _format = conn[1]
                            _sock = conn[0]
                            dic = {"method" : "publish", "topic": topic, "message" : msg}
                            self.send(_sock, dic, _format)


            elif dic_msg["method"] == "list_topics":
                lst = self.list_topics()
                dic = {"method" : "list_topics", "topic":  None, "message": lst}
                self.send(conn, dic, self.total_sock[conn]) 
                    

            elif dic_msg["method"] == "cancel":
                topic = dic_msg["topic"]
                self.topic_dic[topic].remove(conn)
                print(f">> subscrição cancelada do topico {topic}")

        else:
            print(">> one socket has left")
            conn.close()
            
    # converte o dicionário para o formato correto
    def msg_converter(self, msg, serializer):

        if serializer == Serializer.JSON:
            return json.loads(msg)

        elif serializer == Serializer.PICKLE:
            return pickle.loads(msg)

        elif serializer == Serializer.XML:
            tree = XMLTree.fromstring(msg)

            # transformar a mensagem num dicionário
            dic = {}
            for child in tree:
                dic[child.tag] = child.attrib["value"]

            return dic

        else:
            return None


    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        return list(self.topic_dic.keys())

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        print("TOPIC_DIC", self.topic_dic)
        if topic in list(self.topic_dic.keys()):
            return self.topic_dic[topic]
            
        return None


    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topic_dic[topic] = value


    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        return self.topic_subs[topic]


    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if topic in list(self.topic_subs.keys()):
            lst = self.topic_subs[topic]
            lst.append( (address, _format) )
            self.topic_subs[topic] = lst

        else:
            self.topic_subs[topic] = [ (address, _format) ]

        # enviar a ultima mensagem do topic
        if topic in list(self.topic_dic.keys()):
            dic = {"method" : "lastMessage", "topic": topic, "message" : self.topic_dic[topic]}
            print(f">> última mensagem do tópico {topic} enviada")
            self.send(address, dic, _format)

    # envia as mensagens 
    def send(self, conn, msg, _format):

        if _format == Serializer.JSON:
            encoded_msg = json.dumps(msg).encode("utf-8")

        elif _format == Serializer.PICKLE:
            encoded_msg = pickle.dumps(msg)

        elif _format == Serializer.XML:
            root = XMLTree.Element('root')

            for key in msg.keys():
                XMLTree.SubElement(root, str(key)).set("value", str(msg[key]))

            encoded_msg = XMLTree.tostring(root)

        header = len(encoded_msg).to_bytes(2, "big")
        conn.send(header + encoded_msg)


    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        for sub in self.topic_subs[topic]:
            if sub[0] == address:
                self.topic_subs[topic].remove(sub)


    def run(self):
        """Run until canceled."""
        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
