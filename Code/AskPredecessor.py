from threading import Thread
from Commons import sleep_for, sendRequest, refresh_pointers_frequency

#SERVING THREAD of a node
# Periodically inquiries for the node's predecessor. If it fails, it clears node's predecessor
class AskPredecessor(Thread):

    # constructor
    def __init__(self, _node):
        Thread.__init__(self)
        self.local_node = _node
        self.alive = True

# main thread run procedure
    def run(self):
        while self.alive:
            predecessor = self.local_node.getPredecessor()
            if predecessor is not None:
                response_from_predecessor = sendRequest(predecessor, "AreYouThere")
                if response_from_predecessor is None or response_from_predecessor != "IAmAlive":
                    self.local_node.clearPredecessor()
                sleep_for(refresh_pointers_frequency)

    def activate(self):
        self.start()

    def prepare_to_die(self):
        self.alive = False
