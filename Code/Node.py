import sys
from threading import Thread, RLock
import socket

from AskMyRSuccessors import AskMyRSuccessors
from Hashing import hash_key
from PortListener import PortListener
from AskPredecessor import AskPredecessor
from FixFingers import FixFingers
from InetSocketAddress import InetSocketAddress
from Stabilizer import Stabilizer
from Commons import synchronized_with_attr, sendRequest, requestAddress, computeRelativeId, i_th_start, \
    print_finger_table, get_sent_header, delimiter, buffer_size, init, AllNodes, createSocketAddress


# Node constructor with the specified address
# failure recovery: if data replication is required
# _init:  initialization phase or normal operation
class Node(Thread):
    def __init__(self, address, _r, _init=False):
        Thread.__init__(self)
        self.alive = True                               # Is node alive?
        self.lock1 = RLock()                            # Node critical point mechanism 1
        self.lock2 = RLock()                            # Node critical point mechanism 2
        self.lock3 = RLock()                            # Node critical point mechanism 3
        self.localAddress = address                     # InetSocketAddress(ip, port) of the node
        self.failure_recovery = _r > 0                  # Is failure recovery requested?
        self.r = _r                                     # MySuccessors' list length if failure recovery is required
        self.local_id = address.hash()                  # node's identifier
        self.print_finger_table = print_finger_table    # Should Finger Table be printed with other node's info
        self.init_phase = _init                         # Is it the init phase of the Chord ring
        self.predecessor = None                         # predecessor InetSocketAddress of the node
        self.data = dict()                              # dictionary for storing local (key, value) data pairs
        self.FingerTable = dict()                       # dictionary to hold Finger Table of the node
        self.MySuccessors = []                          # List of my R successors (if failure recovery is required)

        #Initializing all the 32 Finger Tables entries (Nullify fingers)
        for finger_index in range(1, 33):
            self.FingerTable[finger_index] = None  # Finger Table contains InetsocketAddresses

        # Initializing node's Serving threads
        self.MyPortListener = PortListener(self, self.failure_recovery)
        if self.failure_recovery:
            self.AskMyRSuccessors = AskMyRSuccessors(self)
        self.MyStabilizer = Stabilizer(self)
        self.MyFixFinger = FixFingers(self)
        self.MyAscPredecessor = AskPredecessor(self)

#return the ip address/port of the node
    def getAddress(self):
        return self.localAddress

#display all Nodes' data using AllNodes (centrally)
    def DisplayNetworkC(self):
        global AllNodes
        for indx, node in AllNodes.items():
            node.printMyData()

#main node's thread running
    def run(self):
        while self.alive:
            pass

#getter for node's local id
    def getId(self):
        return self.local_id

# getter for node's predecessor id
    def getPredecessor(self):
        return self.predecessor

# setter for node's predecessor:
# synhronized: in order each node's instance to execute this method
#              as a critical section
    @synchronized_with_attr("lock1")
    def setPredecessor(self, new_predecessor):
        self.predecessor = new_predecessor

#Nullify node's predecessir
    def clearPredecessor(self):
        self.setPredecessor(None)

# activate all node's servings threads
# and the node itself
    def activate(self):
        self.MyPortListener.activate()
        self.MyStabilizer.activate()
        self.MyFixFinger.activate()
        self.MyAscPredecessor.activate()
        if self.failure_recovery:
            print("Activating ...")
            self.AskMyRSuccessors.activate()
        self.start()
        # self.join()

# print some basic node's info
    def prn(self):
        print(f"identifier = {self.local_id}, ip={self.localAddress.getHost()} , port= {self.localAddress.getPort()}")

# send a message to node's successor to tell it that
# its predecessor has changed to the current node
    def notify(self, successor):
        if successor is not None and not successor.equals(self.localAddress):
            return sendRequest(successor, "IAmYourNewPredecessor_" + \
                               self.localAddress.getHost() + ":" + \
                               str(self.localAddress.getPort()))
        else:
            return None

# Update node's predecessor after receiving a notify message
# to update current node's predecessor
    def notified(self, new_predecessor):
        if self.predecessor is None or self.predecessor.equals(self.localAddress):
            self.setPredecessor(new_predecessor)
        else:
            old_predecessor_id = self.predecessor.hash()
            local_relative_id = computeRelativeId(self.local_id, old_predecessor_id)
            new_predecessor_relative_id = computeRelativeId(new_predecessor.hash(), old_predecessor_id)
            if new_predecessor_relative_id > 0 and new_predecessor_relative_id < local_relative_id:
                self.setPredecessor(new_predecessor)

#updates an entry in the node's Finger Table
    def update_i_th_finger(self, ft_index, new_node_addr):
        self.FingerTable[ft_index] = new_node_addr
        # if the updated entry is the successor then notify the new successor
        if ft_index==1 and self.failure_recovery:
            self.FillMySuccessors()
        if ft_index == 1 and new_node_addr is not None and not new_node_addr.equals(self.localAddress):
            self.notify(new_node_addr)

#delete an address (finger) reference throughout node's Finger Table
    def deleteCertainFinger(self, value_to_be_deleted):
        for indx, addr in self.FingerTable.items():
            if addr is not None and addr.equals(value_to_be_deleted):
                self.FingerTable[indx] = None

#When failure_recovery is required propagate to a request to my R successors
    def propagatePairToMyRSuccessors(self, request):
        if self.failure_recovery: #propagate this pair to all of my r successors also
            for successor in self.MySuccessors:
                try:
                    if successor: sendRequest(successor, request)
                    else:break
                except:
                    print(f"Error in propagate...")
                    break

# getter for node's successor
    def getSuccessor(self):
        return self.FingerTable[1]

# Try to find node's new successor with an appropriate finger or node's predecessor
    def fillSuccessor(self):
        successor = self.getSuccessor()

        if successor is None or successor.equals(self.localAddress):
            for idx_finger in range(2, 33):
                i_th_finger = self.FingerTable[idx_finger]
                if i_th_finger is not None and not i_th_finger.equals(self.localAddress):
                    for j in range(idx_finger - 1, 0, -1):
                        self.update_i_th_finger(j, i_th_finger)
                    break

        successor = self.getSuccessor()
        if (successor is None or successor.equals(self.localAddress)) and \
                self.predecessor is not None and \
                not self.predecessor.equals(self.localAddress):
            self.update_i_th_finger(1, self.predecessor)

# Delete successor and all the Finger Table references to it. It also tries to update it
    def deleteSuccessor(self):
        successor = self.getSuccessor()
        if successor is None: return
        self.deleteCertainFinger(successor)  # delete all successor occurrences in FT
        if self.predecessor is not None and self.predecessor.equals(successor):
            self.setPredecessor(None)
        self.fillSuccessor()
        successor = self.getSuccessor()
        # if successor is still None or local node and the predecessor is a different node
        # keep asking its predecessor to find current node's new successor
        if (successor is None or successor.equals(successor)) and \
                self.predecessor is not None and not self.predecessor.equals(self.localAddress):
            pred = self.predecessor
            prev_pred = None
            while True:
                prev_pred = requestAddress(pred, "WhichIsYourPredecessor_")
                if prev_pred is None: break
                # if pred's predecessor Node has been deleted or it is the localAddress or
                # pred is current node's new successor break
                if prev_pred.equals(pred) or prev_pred.equals(self.localAddress) or \
                        prev_pred.equals(successor):
                    break
                else:  # keep asking
                    pred = prev_pred
            # finnaly update successor
            self.update_i_th_finger(1, pred)

# Synchronized update of a Finger Table entry (ft_index) with a new value
    @synchronized_with_attr("lock2")
    def updateFingers(self, ft_index, value):
        if ft_index >= 1 and ft_index < 32: #update an existing finger to contain a new InetSocketAddress
            self.update_i_th_finger(ft_index, value)
        elif ft_index == -1:  # delete successor
            self.deleteSuccessor()
        elif ft_index == -2:  # delete a specific finger within finger table
            self.deleteCertainFinger(value)
        elif ft_index == -3:  # fill successor
            self.fillSuccessor()

# move some of my keys to my predecessor during a new node join
    def move_keys(self, to_address):
        my_predecessor_addr = to_address
        #my_predecessor_addr = self.getPredecessor()
        #if my_predecessor_addr is None: return
        #my_predecessor_addr_hash = self.getPredecessor().hash()
        my_predecessor_addr_hash = to_address.hash()
        remove_indexes = []
        if not my_predecessor_addr.equals(self.localAddress) and len(self.data.keys()) > 0:
            for indx, value in self.data.items():
                #key_relative_id = hash_key(indx)
                key_relative_id = indx
                if key_relative_id > 0 and key_relative_id <= my_predecessor_addr_hash:
                    #request = get_sent_header(indx) + value
                    resp = sendRequest(my_predecessor_addr, get_sent_header(indx) + value)
                    if resp == 'DataReceptionDone':
                        remove_indexes.append(indx)
            # remove the sent local data
            for ind in remove_indexes:
                del self.data[ind]

#With Failure Recovery fill r elements of MySuccessors List
    def FillMySuccessors(self):
        global r
        next = self.getSuccessor()
        self.MySuccessors = []
        for indx in range(0, self.r):
            self.MySuccessors.append(next)
            if indx< self.r - 1:
                response = sendRequest(next, "WhichIsYourSuccessor")
                if response is not None:
                    try:
                        next = createSocketAddress(response.split("_")[1])
                    except:
                        pass
        self.init_phase = False

# Join of a new node through contact node
# return True if node is successfully added or False otherwise
    def chordjoin(self, contact_node):
        # if contact is an existing node, try to contact it
        my_successor = None
        contact_node_address = contact_node.getAddress()
        if contact_node_address is not None and not contact_node_address.equals(self.localAddress):
            my_successor = requestAddress(contact_node_address, "FindSuccessor_" + str(self.local_id))

        if my_successor is None:
            #print(f"There is no node with such an address {contact_node_address.prn()} in the current ring...")
            return False
        self.updateFingers(1, my_successor)
        if not self.init_phase:
            request = "MoveKeys_" + self.localAddress.to_string()
            sendRequest(my_successor, request)
        self.init_phase = False
        return True

#Move all node's keys to node's successor when current node is leaving
    def move_all_my_keys_to_my_successor(self, replicate):
        my_successor_addr = self.getSuccessor()
        for key, value in self.data.items():
            header = "SentData_" + delimiter + str(key) + delimiter
            request = (header + value)[0:buffer_size]
            if not sendRequest(my_successor_addr, request).startswith("DataReceptionDone"):
                return False
        if not replicate : self.data.clear()
        return True

#Code to be executed when a node is leaving
    def chordleave(self, AllNodes):
        # shut down all inner threads except PortListener
        # and AskMyRSuccessor for the last requests
        self.MyStabilizer.prepare_to_die()
        self.MyFixFinger.prepare_to_die()
        self.MyAscPredecessor.prepare_to_die()
        resp_from_my_succ = sendRequest(self.getSuccessor(), "IAmLeavingAndYourNewPredecessorIs_" + \
                                        self.getPredecessor().to_string()).startswith("Done")
        resp_from_my_pred = sendRequest(self.getPredecessor(), "IAmLeaving_" + \
                                        self.localAddress.to_string()).startswith("Done")
        if resp_from_my_succ and resp_from_my_pred:
            if self.move_all_my_keys_to_my_successor(False): #its not a replication
                self.prepare_to_die()
                return True
        return False

# detect the closest preceding finger within node's Finger Table
# for the search_id identifier
# Returns the closest finger to the search id
    def closest_preceding_finger(self, search_id):
        search_node_id_relative_id = computeRelativeId(search_id, self.local_id)
        for i in range(32, 0, -1):
            i_th_finger = self.FingerTable[i]  # Finger Table contains InetsocketAddresses
            if i_th_finger is None: continue
            i_th_finger_id = i_th_finger.hash()
            i_th_finger_relative_id = computeRelativeId(i_th_finger_id, self.local_id)
            if i_th_finger_relative_id > 0 and i_th_finger_relative_id < search_node_id_relative_id:
                response = sendRequest(i_th_finger, "AreYouThere_")
                if response is not None and response == "IAmAlive":
                    return i_th_finger
                #else:
                #    self.updateFingers(-2, i_th_finger) #remove it from the finger table
        return self.localAddress


# detect the predecessor of an identifier search_id
# Returns search_id's predecessor InetSocketAddress(ip, port)
    def find_predecessor(self, search_id):
        node_addr = self.localAddress
        node_successor_addr = self.getSuccessor()
        recently_alive_addr = self.localAddress
        successor_relative_id = 0

        if node_successor_addr is not None:
            successor_relative_id = computeRelativeId(node_successor_addr.hash(), node_addr.hash())
        search_node_id_relative_id = computeRelativeId(search_id, node_addr.hash())

        while not (search_node_id_relative_id > 0 and search_node_id_relative_id <= successor_relative_id):
            pred_node_addr = node_addr
            if node_addr.equals(self.localAddress): # searching within current node
                node_addr = self.closest_preceding_finger(search_id)
            else: # forward request : Hop
                response_addr = requestAddress(node_addr, "FindClosest_" + str(search_id))
                if response_addr is None:
                    node_addr = recently_alive_addr
                    node_successor_addr = requestAddress(node_addr, "WhichIsYourSuccessor_")
                    if node_successor_addr is None:
                        return self.localAddress
                    continue
                elif response_addr.equals(node_addr): # if current node is its closest also
                    return response_addr
                else: # forward request to response_addr node which is closer - Hop
                    recently_alive_addr = node_addr
                    node_successor_addr = requestAddress(response_addr, "WhichIsYourSuccessor_")
                    if node_successor_addr is not None:
                        node_addr = response_addr
                    else: # forward request to the successor of response_addr node which is closer - Hop
                        node_successor_addr = requestAddress(node_addr, "WhichIsYourSuccessor_")
                if node_successor_addr is None: continue
                # compute relative ids to verify that closest node has been found to terminate while loop
                successor_relative_id = computeRelativeId(node_successor_addr.hash(), node_addr.hash())
                search_node_id_relative_id = computeRelativeId(search_id, node_addr.hash())
            if pred_node_addr.equals(node_addr):
                break
        return node_addr

# Query for local_id's successor
# Returns local_id's successor InetSocketAddress(ip, port)
    def find_successor(self, local_id):
        successor = self.getSuccessor()
        pred = self.find_predecessor(local_id)
        # if some other address has been found ask it for its successor
        if not pred.equals(self.localAddress): # ask predecessor who is its successor
            successor = requestAddress(pred, "WhichIsYourSuccessor_")
        if successor is None: successor = self.localAddress
        return successor

# Stop all node's servings threads preparing node's shutdown
    def stopNodeThreads(self):
        if self.MyPortListener is not None: self.MyPortListener.prepare_to_die()
        if self.failure_recovery and self.AskMyRSuccessors is not None: self.AskMyRSuccessors.prepare_to_die()
        if self.MyFixFinger is not None: self.MyFixFinger.prepare_to_die()
        if self.MyStabilizer is not None: self.MyStabilizer.prepare_to_die()
        if self.MyAscPredecessor is not None: self.MyAscPredecessor.prepare_to_die()
        self.prepare_to_die()

#Print node's info and some of my neighbors' info
    def printMeAndMyNeighbors(self):
        print(f"I am {self.localAddress.getHost()} and I am listening on port {self.localAddress.getPort()}")
        successor = self.getSuccessor()
        if (self.predecessor is None or self.predecessor.equals(self.localAddress)) and \
                (successor is None or successor.equals(self.localAddress)):
            print("\tMy predecessor is me")
            print("\tMe successor is also me")
        else:
            if self.predecessor is not None:
                print(f"\tMy predecessor is {self.predecessor.to_string()}")
            else:
                print("\tMy predecessor is not ready yet...")
            if successor is not None:
                print(f"\tMy successor is {successor.to_string()}")
            else:
                print("\tMy successor is not ready yet...")
        if self.failure_recovery: self.printMySuccessorList()

# Print whole node's Finger Table
    def printMyFingerTable(self):
        # print(f"type = {type(self.FingerTable)}, {self.FingerTable}")
        print("\t\t---------------------------------------------")
        print(f"\t\t------------ Finger Table -------------------")
        print("\t\t---------------------------------------------")
        for indx, finger_addr in self.FingerTable.items():
            if finger_addr is None:
                print(f"\t\t{str(indx).zfill(2)} finger : NULL")
            else:
                print(f"\t\t{str(indx).zfill(2)} finger : {finger_addr.to_string()}")

#Print node stored data pairs (key, value)
    def printMyKeys(self):
        print("\t\t---------------------------------------------")
        print(f"\t\t------------  Local keys  -------------------")
        print("\t\t---------------------------------------------")
        for key, value in self.data.items():
            print(f"\t\tkey:{key}, value : {value}")

# When failure recovery is required, printing node's successor list
    def printMySuccessorList(self):
        print(f"\nI am {self.localAddress.to_string()} and my list of successors is : ", end='')
        for indx, succ in enumerate(self.MySuccessors):
            if indx < len(self.MySuccessors) - 1: print(succ.to_string(), ' -> ', end='')
            else: print(succ.to_string())

# Synchonized printing of node's info
    @synchronized_with_attr("lock3")
    def printMyData(self):
        self.printMeAndMyNeighbors()
        if self.print_finger_table: self.printMyFingerTable()
        self.printMyKeys()

# Preparing node to stop executing its main run procedure
    def prepare_to_die(self):
        self.alive = False
