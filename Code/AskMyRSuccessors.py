from threading import Thread
from Commons import sleep_for, sendRequest, createSocketAddress

# SERVING THREAD of a node
# Tries to fill the R Successors List of a node
# created by a node only if failure recovery is required by the user
class AskMyRSuccessors(Thread):
    # constructor
    def __init__(self, _node):
        Thread.__init__(self)
        self.local_node = _node
        self.alive = True

# main thread run procedure
    def run(self):
        while self.alive:
            next_successor = self.local_node.getSuccessor()
            self.local_node.MySuccessors = []
            for indx in range(0, self.local_node.r):
                if next_successor is not None:
                    self.local_node.MySuccessors.append(next_successor)
                    response_from_successor = sendRequest(next_successor, "WhichIsYourSuccessor_")
                    if response_from_successor is not None and response_from_successor.startswith("RequiredSuccessorIs_"):
                        next_successor = createSocketAddress(response_from_successor.split("_")[1])
                else : break
            sleep_for(1.5)

    def activate(self):
        self.start()

    def prepare_to_die(self):
        self.alive = False
