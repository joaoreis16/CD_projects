"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
import socket, sys
from typing import Any
import json, pickle
import xml.etree.ElementTree as XMLTree

class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.type = _type
        self.topic = topic

        self.middle_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        host = 'localhost'
        port = 5000
        self.middle_sock.connect( (host, port) )
        print(f"connected to ({host},{port})")


    def push(self, value):
        """Sends data to broker. """
        header = len(value).to_bytes(2, "big")
        self.middle_sock.send(header + value)


    def pull(self) -> (str, Any):
        """Waits for (topic, data) from broker.

        Should BLOCK the consumer!"""
        header = int.from_bytes(self.middle_sock.recv(2) , "big")
        if header == 0: return

        msg = self.middle_sock.recv(header)
        return msg, None

        
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        dic = {"method" : "list_topics"}
        self.push(dic)


    def cancel(self):
        """Cancel subscription."""
        dic = {"method" : "cancel", "topic" : self.topic }
        self.push(dic)


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)

        dic = {"method": "ack", "format": "json"}
        msg = pickle.dumps(dic)
        super().push(msg) # send type of messages

        if _type == MiddlewareType.CONSUMER:
            dic = {"method": "subscribe", "topic": topic }
            encoded_msg = json.dumps(dic).encode("utf-8")
            super().push(encoded_msg)


    # send a publish message to broker
    def push(self, value):
        print(f"value: {value} | topic: {self.topic}")
        dic = {"method": "publish", "topic": self.topic, "message": value}
        encoded_msg = json.dumps(dic).encode("utf-8")
        super().push(encoded_msg)


    # receive messages from broker
    def pull(self) -> (str, Any):
        msg, temp = super().pull()
        dic_msg = json.loads(msg)

        print(f'value: {dic_msg["message"]} | topic: { dic_msg["topic"]}')
        return dic_msg["topic"], dic_msg["message"]
        

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""

    def __init__(self,topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)

        dic = {"method": "ack", "format": "xml"}
        msg = pickle.dumps(dic)
        super().push(msg) # send type of messages
        
        if _type == MiddlewareType.CONSUMER:
            root = XMLTree.Element('root')
            XMLTree.SubElement(root, 'method').set("value", "subscribe")
            XMLTree.SubElement(root, 'topic').set("value", str(topic))
            encoded_msg = XMLTree.tostring(root)
            super().push(encoded_msg) 

    
    # send a publish message to broker
    def push(self, value):
        print(f"value: {value} | topic: {self.topic}")
        root = XMLTree.Element('root')
        XMLTree.SubElement(root, 'method').set("value", "publish")
        XMLTree.SubElement(root, 'topic').set("value", self.topic)
        XMLTree.SubElement(root, 'message').set("value", str(value))

        super().push(XMLTree.tostring(root))

    # receive messages from broker
    def pull(self) -> (str, Any):
        msg, temp = super().pull()
        tree = XMLTree.fromstring(msg)

        dic_msg = {}
        for child in tree:
            dic_msg[child.tag] = child.attrib["value"]

        print(f'value: {dic_msg["message"]} | topic: { dic_msg["topic"]}')
        return dic_msg["topic"], dic_msg["message"]


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)

        dic = {"method": "ack", "format": "pickle_ric"}
        msg = pickle.dumps(dic)
        super().push(msg) # send type of messages

        if _type == MiddlewareType.CONSUMER:
            dic = {"method": "subscribe", "topic": topic }
            encoded_msg = pickle.dumps(dic)
            super().push(encoded_msg )


    # send a publish message to broker
    def push(self, value):
        print(f"value: {value} | topic: {self.topic}")
        dic = {"method": "publish", "topic": self.topic, "message": value}
        encoded_msg = pickle.dumps(dic)
        super().push(encoded_msg)

    # receive messages from broker
    def pull(self) -> (str, Any):
        msg, temp = super().pull()
        dic_msg = pickle.loads(msg)

        print(f'value: {dic_msg["message"]} | topic: { dic_msg["topic"]}')
        return dic_msg["topic"], dic_msg["message"]
