"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket


class Message:
    """Message Type."""

    def __init__(self, command):
        self.command = command
    
    def __repr__(self):
        return f'"command": "{self.command}"'


class JoinMessage(Message):
    """Message to join a chat channel."""

    def __init__(self, command, channel):
        super().__init__(command)
        self.channel = channel
        pass

    def __repr__(self):
        return f'{{{super().__repr__()}, "channel": "{self.channel}"}}'


class RegisterMessage(Message):
    """Message to register username in the server."""

    def __init__(self, command, user):
        super().__init__(command)
        self.user = user
    
    def __repr__(self):
        return f'{{{super().__repr__()}, "user": "{self.user}"}}'


class TextMessage(Message):
    """Message to chat with other clients."""

    def __init__(self, command, message, ts, channel = None):
        super().__init__(command)
        self.message = message
        self.channel = channel
        self.ts = ts
    
    def __repr__(self):
        if self.channel:
            return f'{{{super().__repr__()}, "message": "{self.message}", "channel": "{self.channel}", "ts": {self.ts}}}'
        else:
            return f'{{{super().__repr__()}, "message": "{self.message}", "ts": {self.ts}}}'


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""

        return RegisterMessage("register", username)
    
    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""

        return JoinMessage("join", channel)

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""

        return TextMessage("message", message, int(datetime.now().timestamp()), channel)

    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""

        if type(msg) is RegisterMessage:
            json_msg = json.dumps({"command": "register", "user": msg.user}).encode("UTF-8")

        elif type(msg) is JoinMessage:
            json_msg = json.dumps({"command": "join", "channel": msg.channel}).encode("UTF-8")

        elif type(msg) is TextMessage:
            json_msg = json.dumps({"command": "message", "message": msg.message, "channel": msg.channel, "ts": int(datetime.now().timestamp()) }).encode("UTF-8")

        header = len(json_msg).to_bytes(2, "big")
        connection.sendall(header + json_msg)
        

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""

        try:

            header = int.from_bytes(connection.recv(2), "big")
            if header == 0: return
            
            dic_msg = connection.recv(header).decode("UTF-8")

            dic = json.loads(dic_msg)

        except json.JSONDecodeError as err:
            raise CDProtoBadFormat(dic_msg)

        if dic["command"] == "register":
            user = dic["user"]
            return CDProto.register(user)

        elif dic["command"] == "join":
            channel = dic["channel"]
            return CDProto.join(channel)

        elif dic["command"] == "message":
   
            msg = dic["message"]

            if dic.get("channel"):  # caso haja canal ou não
                return CDProto.message(msg, dic["channel"])

            else:
                return CDProto.message(msg)
            
        
class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
