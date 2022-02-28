from numpy import uint32
from sys import exc_info
import socket
from time import sleep
from InetSocketAddress import InetSocketAddress
import hashlib

#Global variables
buffer_size = 1024                  # buffer size for message interchanging
print_finger_table = True           # printing finger table while printing node's info
delimiter = "@$-$@"                 # delimiter to split sent request
init = True                         # True during Chord Ring init phased
AllNodes = dict()                   # Contains all Chord Ring nodes mainly for network displaying
stabilization_frequency = 0.9       # How often stabilization procedure runs in seconds
refresh_pointers_frequency = 0.9    # How often AskPredecessor, FixFingers will be invoked in seconds
socket_time_out = 5                 # Seconds to wait for a socket request
socket_response_waiting_time = 0.1  # Waiting time for an answer to be sent in seconds

# returns a header for sending data request
def get_sent_header(hashed_id):
    return "SentData_" + delimiter + str(hashed_id) + delimiter

# returns a header for sending a delete data request
def get_delete_header(hashed_id):
    return "DeleteData_" + str(hashed_id) + delimiter

# returns a header for sending an update data request
def get_update_header(hashed_id):
    return "UpdateData_" + str(hashed_id) + delimiter

# get a string containing an address and build is corresponding InetSocketAddress
def createSocketAddress(address):
    lst = address.split(":")
    if len(lst) == 2:
        host = lst[0]
        port = lst[1]
        if host.startswith("/"): host = host[1:]
        ip_parts = host.split(".")
        if len(ip_parts) != 4: return None
        try:
            for addr in ip_parts:
                n = int(addr)
                if n < 0 or n > 255: raise ValueError
            iport = int(port)
            return InetSocketAddress(host, iport)
        except:
            print("Invalid ip address....")
            return None
    else:
        return None


#generic definition of synchronization encapsulation of methods
def synchronized_with_attr(lock_name):
    def decorator(method):
        def synced_method(self, *args, **kws):
            lock = getattr(self, lock_name)
            with lock:
                return method(self, *args, **kws)
        return synced_method
    return decorator

# sleep a process for some seconds
def sleep_for(secs):
    try:
        sleep(secs)
    except:
        print(f"{exc_info()[0]} exception occurred.")

# Asking for the address of a node using a request
def requestAddress(server_addr, request):
    if server_addr is None or request is None: return None
    response = sendRequest(server_addr, request)
    if response is None: return None
    if response.startswith("Nothing"): return None
    return createSocketAddress(response.split("_")[1])

# send a request to a node with specific address
def sendRequest(server_node_address, request):
    global buffer_size
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(socket_time_out)
        try:
            s.connect((server_node_address.getHost(), server_node_address.getPort()))
            s.sendall(bytes(request, 'utf-8'))
            sleep_for(socket_response_waiting_time)
            data = s.recv(buffer_size)
            s.close()
            response_from_server = data.decode('utf-8')
            if response_from_server=="None": return None
            return response_from_server
        except:
            return None

#return the start of the i-th finger
def i_th_start(nodeid, offset):
    return uint32((nodeid + 2**(offset-1)) % (1 << 32)) #1<<32 = 2^32 but faster

# Compute the relative distance of two identifiers
def computeRelativeId(id1, id2):
    try:
        ret = int(id1) - int(id2)
        if ret < 0: ret += 1<<32 #(2^32 but faster)
        return uint32(ret)
    except:
        print(f"Error in CompRelId for :id1={id1}, id2={id2}")

