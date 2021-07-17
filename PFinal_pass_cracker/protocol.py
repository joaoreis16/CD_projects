import json

class Protocol:
    """Computação Distribuida Protocol."""

    @classmethod
    def join(cls, addr):
        dic =  {"command" : "join"}
        dic["address"] = addr
        return json.dumps(dic).encode("utf-8")
    
    @classmethod
    def distribute_work(cls, work):
        dic =  {"command" : "distribute"}
        dic["work"] = work
        return json.dumps(dic).encode("utf-8")

    @classmethod
    def found(cls, work, password):
        dic =  {"command" : "found"}
        dic["work"] = work
        dic["password"] = password
        return json.dumps(dic).encode("utf-8")
