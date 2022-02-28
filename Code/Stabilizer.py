from threading import Thread
from Commons import requestAddress, computeRelativeId, sleep_for, stabilization_frequency
from InetSocketAddress import InetSocketAddress

# SERVING THREAD of a node
# Periodically inquiry node's successor for its predecessor deciding
# in this way if current node should update its successor
class Stabilizer(Thread):

    # constructor
    def __init__(self, _node):
        Thread.__init__(self)
        self.local_node = _node
        self.alive = True

# main thread run procedure
    def run(self):
        while self.alive:
            successor = self.local_node.getSuccessor()
            if successor is None or successor.equals(self.local_node.getAddress()):
                self.local_node.updateFingers(-3, None)  # fill successor
            successor = self.local_node.getSuccessor()
            if successor is not None and not successor.equals(self.local_node.getAddress()):
                # trying to get my successor's predecessor
                candidate = requestAddress(successor, "WhichIsYourPredecessor")
                if candidate is None: #successor failed to respond then delete successor
                    self.local_node.updateFingers(-1, None)
                elif not candidate.equals(successor): #successor's predecessor has changed
                    succ_rel_id = computeRelativeId(successor.hash(), self.local_node.local_id)
                    cand_rel_id = computeRelativeId(candidate.hash(), self.local_node.local_id)
                    if cand_rel_id > 0 and cand_rel_id < succ_rel_id:
                        self.local_node.updateFingers(1, candidate) #change my successor
                else:
                    self.local_node.notify(successor) #notify my successor that its predecessor has changed
            sleep_for(stabilization_frequency)

    def activate(self):
        self.start()

    def prepare_to_die(self):
        self.alive = False
