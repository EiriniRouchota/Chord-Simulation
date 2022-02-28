import sys
import hashlib
from numpy import uint32, frombuffer, dtype, random, uint8
from Hashing import hash_common

#define a class for InetSocketAddress = (ip, port)
#and some related methods

class InetSocketAddress:
    def __init__(self, _host, _port):
        self.address = (str(_host), _port)

    def __init(self, address):
        addr = address.split(":")
        self.address = str(address)

    def getHost(self):
        return self.address[0]

    def getPort(self):
        return self.address[1]

    def equals(self, other):
        return isinstance(other, InetSocketAddress) and \
               self.getHost() == other.getHost() and \
               self.getPort() == other.getPort()

    def hash(self):
        mstr = self.getHost() + ":" + str(self.getPort())
        return hash_common(mstr)

    def prn(self):
        print(f"My ip is {self.address[0]} and my listening port is {self.address[1]}")

    def to_string(self):
        return self.getHost() + ":" + str(self.getPort())
