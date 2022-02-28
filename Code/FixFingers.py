from threading import Thread
import random
from Commons import sleep_for, i_th_start, refresh_pointers_frequency

# SERVING THREAD of a node
# Periodically select a random finger and recompute its reference
class FixFingers(Thread):

    # constructor
    def __init__(self, _node):
        Thread.__init__(self)
        self.local_node = _node
        self.alive = True

# main thread run procedure
    def run(self):
        while self.alive:
            i = random.randint(2, 32) # not including successor = FT[1]
            i_th_finger = self.local_node.find_successor(i_th_start(self.local_node.getId(), i))
            self.local_node.updateFingers(i, i_th_finger)
            sleep_for(refresh_pointers_frequency)

    def activate(self):
        self.start()

    def prepare_to_die(self):
        self.alive = False
