import selectors
import socket
import types
from threading import Thread
from numpy import uint32
from Commons import createSocketAddress, sendRequest, delimiter, buffer_size

# SERVING THREAD of a node
# Main communication thread of a node
# Listening the port of a node, for requests from others nodes
# Upon the reception  and the process of a request, it also sent an answer to the requesting node

class PortListener(Thread):

    #constructor
    def __init__(self, _node, _failure_recovery):
        Thread.__init__(self)
        self.local_node = _node
        self.alive = True
        self.localAddress = self.local_node.getAddress()
        self.port = self.localAddress.getPort()
        self.host = self.localAddress.getHost()

        self.selector = selectors.DefaultSelector()
        lsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsocket.bind((self.host, self.port))
        lsocket.listen()
        # print('MServer is listening on:', (self.host, self.port))
        lsocket.setblocking(False)
        self.selector.register(lsocket, selectors.EVENT_READ, data=None)

    def processReguest(self, request):
        global delimiter

        if request is None: return None
        result = None
        ret_val = ""
        if request.startswith("FindClosest"):
            node_id = uint32(request.split("_")[1])
            result = self.local_node.closest_preceding_finger(node_id)
            ip = result.getHost()
            port = result.getPort()
            return "MyClosestIs_" + ip + ":" + str(port)

        if request.startswith("WhichIsYourSuccessor"):
            result = self.local_node.getSuccessor()
            if result is not None:
                ip = result.getHost()
                port = result.getPort()
                return "MySuccessorIs_" + ip + ":" + str(port)
            else:
                return "Nothing"

        if request.startswith("WhichIsYourPredecessor"):
            result = self.local_node.getPredecessor()
            if result is not None:
                ip = result.getHost()
                port = result.getPort()
                return "MyPredecessorIs_" + ip + ":" + str(port)
            else:
                return "Nothing"

        if request.startswith("FindSuccessor_"):
            node_id = uint32(request.split("_")[1])
            result = self.local_node.find_successor(node_id)
            ip = result.getHost()
            port = result.getPort()
            return "RequiredSuccessorIs_" + ip + ":" + str(port)

        if request.startswith("WhichIsYourSuccessor_"):
            my_successor = self.local_node.getSuccessor()
            if my_successor: return "RequiredSuccessorIs_" + self.local_node.localAddress.to_string()
            else: return "None"

        if request.startswith("IAmYourNewPredecessor"):
            new_pred = createSocketAddress(request.split("_")[1])
            self.local_node.notified(new_pred)
            return "Notified"

        if request.startswith("AreYouThere"):
            return "IAmAlive"

        if request.startswith("SentData_"):
            data = request.split(delimiter)
            key = uint32(data[1])
            value = data[2]
            self.local_node.data[key] = value
            if self.local_node.failure_recovery:
                try:
                    arg = data[3] #if 4th argument is present
                    request = data[0] + delimiter + data[1] + delimiter + data[2]
                    self.local_node.propagatePairToMyRSuccessors(request)
                    return "DataReceptionDone"
                except:
                    return "Nothing"
            return "DataReceptionDone"

        if request.startswith("DeleteData_"):
            data = request.split("_")[1]
            entry = data.split(delimiter)
            del self.local_node.data[uint32(entry[0])]
            return "DataDeletionDone"

        if request.startswith("IAmLeaving_"):
            data = request.split("_")[1]
            new_successor_addr = createSocketAddress(data)
            self.local_node.updateFingers(-1, None) #deleteSuccessor
            return "Done"

        if request.startswith("IAmLeavingAndYourNewPredecessorIs_"):
            data = request.split("_")[1]
            new_pred_addr = createSocketAddress(data)
            leaving_node_addr = self.local_node.getSuccessor()
            self.local_node.setPredecessor(new_pred_addr)
            self.local_node.updateFingers(-2, leaving_node_addr) #deleteCertainFinger
            return "Done"

        if request.startswith("PrintYourSelf"):
            self.local_node.printMyData()
            init_addr_str = request.split("_")[1]
            init_addr = createSocketAddress(init_addr_str)
            if not self.local_node.getAddress().equals(init_addr):
                sendRequest(self.local_node.getSuccessor(), request)

        if request.startswith("AskForKey_"):
            hashed_key = request.split("_")[1]
            hashed_key = uint32(request.split("_")[1])
            if hashed_key in self.local_node.data:
                respond = "KeyFound_" + delimiter + self.localAddress.to_string() + delimiter +\
                          str(self.local_node.data[hashed_key])
                return respond[0:buffer_size]
            else:
                return "KeyNotFound"

        if request.startswith("MoveKeys_"):
            target_address_str = request.split("_")[1]
            target_addr = createSocketAddress(target_address_str)
            self.local_node.move_keys(target_addr)
            return "MoveKeysDone"

        if request.startswith("ShutDownRing"):
            init_addr_str = request.split("_")[1]
            init_addr = createSocketAddress(init_addr_str)
            if not self.local_node.getAddress().equals(init_addr):
                sendRequest(self.local_node.getSuccessor(), request)
            self.local_node.stopNodeThreads()

# getting and serving a connection to the node
    def service_connection(self, key, mask):
        global buffer_size
        clsocket = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            try:
                recv_data = clsocket.recv(buffer_size)  # Should be ready to read
                if recv_data:
                    data.outb += recv_data
                else:
                    # print('closing connection to', data.addr)
                    self.selector.unregister(clsocket)
                    clsocket.close()
            except:
                self.selector.unregister(clsocket)
                clsocket.close()
        if mask & selectors.EVENT_WRITE:
            if data.outb:
                # print(f"{(self.host, self.port)} echoing' {repr(data.outb)} 'to' {data.addr}")
                data.outb = str(self.processReguest(data.outb.decode('utf-8'))).encode()  # , data.addr).
                sent = clsocket.send(data.outb)  # Should be ready to write
                data.outb = data.outb[sent:]

# accepting a socket connection
    def accept_wrapper(self, sock):
        conn, address = sock.accept()  # Should be ready to read
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=address, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.selector.register(conn, events, data=data)

    def activate(self):
        self.start()

# main thread run procedure
    def run(self):
        while self.alive:
            events = self.selector.select(timeout=None)
            # if (events): print(f"Events in {self.host}, {self.port}")
            for key, mask in events:
                if key.data is None:
                    self.accept_wrapper(key.fileobj)
                else:
                    self.service_connection(key, mask)

    def prepare_to_die(self):
        self.alive = False
