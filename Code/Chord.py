import os
import random
import sys
import signal
import tempfile
import time
from pandas import read_csv
from random import randint
from InetSocketAddress import InetSocketAddress
import socket
import argparse

from Node import Node
from Hashing import hash_key
from Commons import sendRequest, buffer_size, AllNodes, delimiter
import graphviz
import re
from traceback import print_exc
from datetime import datetime

# Global variables
m = 32                          # m bits of Chord ring
global n                        # number of init nodes in Chord Ring
initial_port = 60000            # port of the initial node
global stat_id                  # label for statistic analysis created by the exec date and time

global input_file_name          # input csv file name to read data from
global statistics_file_name     # output file name to write operations statistics
global pc_ip                    # local pc ip: All nodes will have the same ip but a different port
global node0_address            # first node to be created
global failure_recovery         # failure recovery is required
global data_records_to_be_read  # How many records should be initially loaded from the input csv file

print_statistics = True         # flag to display statistics info
global init_records             # records loaded from csv input file
global r

global SearchKeys               # ids of the file imported data

#counting time from start point to end point
def elapsed(end, start):
    return (end - start) / 10 ** 9

#return a random node from AllNodes collection
def get_random_node():
    global AllNodes
    random_item = randint(0, len(AllNodes.items())-2)
    return AllNodes[list(AllNodes.keys())[random_item]]

#Check if the given filename is a valid csv file name
def is_a_valid_csv_file_name(fname):
    str_fname = fname.strip().replace(' ', '_')
    if str_fname.endswith('.csv'):
        x = re.search("[a-zA-Z][a-zA-Z0-9_]*\.csv", str_fname)
        return x.group()
    else:
        return None

# sent a (key/value) pair to the contact_node
def load_record_to_node(key, value, contact_node):
    global failure_recovery
    global r
    id = key.to_bytes(4, byteorder='big')
    str_id = str(id)
    hashed_id = hash_key(str_id)
    header = "SentData_" + delimiter + str(hashed_id) + delimiter
    request = (header + str(key) + "->" + value)[0:buffer_size]
    sent_to_node_addr = contact_node.find_successor(hashed_id)
    if sent_to_node_addr.equals(contact_node.localAddress):
        contact_node.data[hashed_id] = (str(key) + "->" + value)[0:buffer_size]
        if failure_recovery: contact_node.propagatePairToMyRSuccessors(request)
    else:
        if failure_recovery: request += delimiter + "_propagate"
        resp = sendRequest(sent_to_node_addr, request)
        if resp != 'DataReceptionDone':
            contact_node.data[hashed_id] = (str(key) + "->" + value)[0:buffer_size]

#delete a (key/value) pair identified by its key from a contact_node
def delete_record_from_node(key, contact_node):
    id = key.to_bytes(4, byteorder='big')
    str_id = str(id)
    hashed_id = hash_key(str_id)
    request = "DeleteData_" + str(hashed_id)
    sent_to_node_addr = contact_node.find_successor(hashed_id)
    if sent_to_node_addr.equals(contact_node.localAddress):
        del contact_node.data[hashed_id]
    else:
        sendRequest(sent_to_node_addr, request)

# load #records_to_load records from csv file on the ring though node
# Each record is being routed to a node using its identifier and successor(key)=node
def load_file_on_the_ring(node, records_to_load):
    global input_file_name
    global delimiter
    global data_records_to_be_read
    global init_records
    global SearchKeys

    SearchKeys = []
    init_records = 0
    try:
        df1 = read_csv(input_file_name)
        df = df1.head(data_records_to_be_read)
        for index, row in df.iterrows():
            display_progress("Loading init data from input file to the ring nodes (through the first Node)", init_records,records_to_load)
            mstr = ""
            for n in range(1, len(df.columns)):
                mstr = mstr + "," + str(row[n])
            SearchKeys.append(int(row[0]))
            load_record_to_node(int(row[0]), mstr[1:], node)
            init_records += 1
    except Exception as e:
        print_exc()
        print(f"Unable to load initial input file...{input_file}")
        exit(-5)

# append str to a local statistics.csv file recording experimental
# measurements of basic chord operations if print_statistics flag is set
def fprint(op_type, elapsed_time):
    global print_statistics
    global stat_id
    if print_statistics:
        print_str = f"{stat_id},{op_type},{len(AllNodes.items())},{elapsed_time}\n"
        #print(str, file=sys.stderr)
        f = open(statistics_file_name, "a")
        f.write(print_str)
        f.close()

#printing main menu options
def printMenuOptions():
    print('=========================================================================')
    print("Type [1] to insert a new (key/value) pair in the Chord ring")
    print("Type [2] to delete a (key/value) pair from the Chord ring")
    print("Type [3] to update value in an existing (key/value) pair")
    print("Type [4] to perform an exact match in the network (Lookup)")
    print("Type [5] to display current Chord ring configuration")
    print("Type [6] to add a new Node (Join) in the Chord ring")
    print("Type [7] to delete an existing Node (Leave) from the Chord ring")
    # print("Type [8] to delete massively existing Nodes from the Chord ring")
    print("Type [8] to run a complete benchmark on the current Chord ring")
    print("Type [0] to exit")
    print('=========================================================================')

    while True:
        try:
            choice = int(input('Select an operation: '))
            if choice >= 0 and choice <= 8:
                return choice
            else:
                raise KeyError
        except:
            print("Invalid input...Try again")

# Insert a new pair asking user to give the key and value of the pair
def insertNewPairCore(key, value):
    start = time.process_time_ns()
    load_record_to_node(key, value, get_random_node())
    end = time.process_time_ns()
    elapsed_time = elapsed(end, start)
    fprint("Insert", elapsed_time)

def InsertNewPair():
    global SearchKeys
    print("Insert new (key/value) pair")
    try:
        key = int(input("Give the key : "))
        value = input("Give the value : ")
        SearchKeys.append(key)
        insertNewPairCore(key, value)
    except Exception as e:
        print(e)
        return

# delete a pair from the corresponding node that is being searched by an identifier
# provided by the user
def DeletePairCore(key):
    start = time.process_time_ns()
    delete_record_from_node(key, get_random_node())
    end = time.process_time_ns()
    elapsed_time = elapsed(end, start)
    fprint("Delete", elapsed_time)

def DeletePair():
    print("Delete an old (key/value) pair")
    try:
        key = int(input("Give the key to be deleted: "))
        DeletePairCore(key)
    except Exception as e:
        print(e)
        return

# update a pair's value from the corresponding node that is being searched by
# an identifier provided by the user
def UpdatePairCore(key, newvalue):
    start = time.process_time_ns()
    load_record_to_node(key, newvalue, get_random_node())
    end = time.process_time_ns()
    elapsed_time = elapsed(end, start)
    stat_str = f"{stat_id},Update,{len(AllNodes.items())},{elapsed_time}"
    fprint("Update", elapsed_time)

def UpdatePair():
    print("Update an old (key/value) pair")
    try:
        key = int(input("Give the key : "))
        newvalue = input("Give the new value : ")
        UpdatePairCore(key, newvalue)
    except Exception as e:
        print(e)

# display Chord Ring network by inquiring each node (by sending requests)
def DisplayNetwork(node):
    print("\nDisplay Network configuration\n")
    successor = node.getSuccessor()
    node_addr = node.getAddress()
    if successor is not None and not successor.equals(node_addr):
        sendRequest(successor, "PrintYourSelf_" + node_addr.to_string())

# display Chord Ring network using AllNodes main data structure
def DisplayNetworkC():
    for indx, node in AllNodes.items():
        node.printMyData()

# add a new node to the Chord Ring notifying it whether r successor should be stored
# if failure recovery is required by the user
def AddNewNode(init_phase):
    global AllNodes
    global next_available_port
    global pc_ip
    global r

    loc_new_node_address = InetSocketAddress(pc_ip, next_available_port)
    next_available_port += 1
    loc_new_node = Node(loc_new_node_address, r, init_phase)
    AllNodes[loc_new_node_address.to_string()] = loc_new_node
    loc_new_node.activate()

    start = time.process_time_ns()
    loc_new_node.chordjoin(AllNodes[node0_address.to_string()])
    end = time.process_time_ns()
    elapsed_time = elapsed(end, start)
    stat_str = f"{stat_id},AddNewNode,{len(AllNodes.items())},{elapsed_time}"
    fprint("AddNewNode", elapsed_time)

    if r>0:
        loc_new_node.FillMySuccessors()
        #loc_new_node.move_keys()

# A new node will be created and is going to join the Chord Ring
def NewNodeJoins():
    global r
    global next_available_port
    print(f"A new node is going to join at the port {next_available_port}")
    AddNewNode(False)

# Implementing single node leave from Chord Ring
def SingleNodeLeave():
    rand_node = get_random_node()
    init = False
    print(f"Node {rand_node.localAddress.to_string()} is leaving...", end='')
    start = time.process_time_ns()
    if rand_node.chordleave(AllNodes):
        del AllNodes[rand_node.localAddress.to_string()]
        print('Done')
    else: print("Error")
    end = time.process_time_ns()
    elapsed_time = elapsed(end, start)
    stat_str = f"{stat_id},Leave,{len(AllNodes.items())},{elapsed_time}"
    fprint("Leave", elapsed_time)

# def MassiveNodeLeaves():
#     print("Massive nodes are leaving")

# Exact Query Implementation (Lookup)
def ExactQueryCore(key):
    random_node = get_random_node()
    id = key.to_bytes(4, byteorder='big')
    str_id = str(id)
    hashed_id = hash_key(str_id)
    start = time.process_time_ns()
    pred_address = random_node.find_successor(hashed_id)
    resp = sendRequest(pred_address, "AskForKey_" + str(hashed_id))
    end = time.process_time_ns()
    elapsed_time = elapsed(end, start)
    stat_str = f"{stat_id},Lookup,{len(AllNodes.items())},{elapsed_time}"
    fprint("Lookup", elapsed_time)
    if resp.startswith("KeyFound_"):
        received_data = resp.split(delimiter)
        received_from_node = received_data[1]
        value = received_data[2]
        print("\tKey has been found in '" + received_from_node + "' node with value : \n\t\t" + value)
    else:
        print(f"\tThere is no such key ({key}) within the ring's nodes")

def ExactQuery():
    print("Performing an exact query (Lookup)")
    try:
        key = int(input("Give the key to search for: "))
        ExactQueryCore(key)
    except Exception as e:
        print(e)
        return

def autobenchmark():
    if not print_statistics: return
    print("Running an automated benchmark for statistics!")
    print("========================================================")
    local_key = random.randint(1000, 2000)
    value = "a string"
    # insert a new random pair (local_key, value)
    print("\tInserting a new pair...", end='')
    insertNewPairCore(local_key, value)
    print("Done")
    # lookup k1
    print("\tLookup previously inserted key...", end='')
    ExactQueryCore(local_key)
    print("Done")
    # update k1 to a new random value
    print("\tUpdate a value in (key, value)....", end='')
    UpdatePairCore(local_key, "another string")
    print("Done")
    # delete k1
    print("\tDelete the newly inserted pair...", end='')
    DeletePairCore(local_key)
    print("Done")
    # insert a new node
    print("\tInsert a new node...", end='')
    NewNodeJoins()
    print("Done")
    # single node leave
    print("\tA node is leaving...", end='')
    SingleNodeLeave()
    print("========================================================")
    print("================== Benchmark completed =================")
    print("========================================================")


# process a user selection from the main menu
def processSelection(selection, node):
    global SearchKeys
    if selection == 1:  # insert new pair (key/value)
        InsertNewPair()
    elif selection == 2:  # delete an old pair (key/value)
        DeletePair()
    elif selection == 3:  # update an old pair (key/value)
        UpdatePair()
    elif selection == 4:  # exact match Query (Lookup)
        ExactQuery()
    elif selection == 5:  # Display Network configuration
        if failure_recovery: fillSuccessorsList()
        DepictNetwork()
        # for idx in SearchKeys:
        #     ExactQueryCore(idx)
    elif selection == 6:  # a new node joins
        NewNodeJoins()
    elif selection == 7:  # Single Leave
        init = False
        SingleNodeLeave()
    # elif selection == 8:  # Massive Leaves
    #     MassiveNodeLeaves()
    elif selection == 8:  # run auto benchmark
        autobenchmark()

#display a text progress meters on various operations
def display_progress(init_str, current, total):
    j = (current + 1) / total
    sys.stdout.write('\r')
    sys.stdout.write('%s : [%-20s] %d%%' % (init_str, '=' * int(20 * j), 100 * j))
    sys.stdout.flush()

# create Network picture using AllNode main data structure using graphvis library.
# It constucts (within a string named plot_data) a dot graph description that is
# depicted using graphviz.Source(plot_data).view()
def DepictNetwork():
    plot_data = "digraph G{\n\rlabelloc=\"t\"\n\rfontsize=\"24\"\n\rlabel=\"Chord Ring - IP : " + pc_ip + "\""
    for ind, node in AllNodes.items():
        plot_data += f"{node.localAddress.getPort()} [fontsize=18]"
        plot_data += f"{node.localAddress.getPort()}->{node.getSuccessor().getPort()} [penwidth=3 color=red]\r\n"
        node_data = "Stored keys:\n--------\n"
        for indx, value in node.data.items():
            val = value.split("->")
            node_data += f"key : {val[0]}\n"
        plot_data += f"data_{node.localAddress.getPort()} [label=\"{node_data}\", shape=box] [fontsize=12]\r\n"
        plot_data += f"data_{node.localAddress.getPort()}->{node.localAddress.getPort()} [arrowhead = none, style=\"dashed\"]\r\n"
    plot_data += "}"
    try:
        graphviz.Source(plot_data).view(tempfile.mktemp('.chord'))
    except Exception as e:
        print(e)

# If failure recovery is required, during the initilization phase only
# the list of R successor is being filled using the AllNodes main data structure
def fillSuccessorsList():
    for indx, node in AllNodes.items():
        node.FillMySuccessors()

def define_args():
    parser = argparse.ArgumentParser(description='Implementing Chord')
    parser.add_argument('-n', type=int, help="N is number of initial numbers of nodes withing Chord ring", default=5)
    parser.add_argument('-fn', type=str, help="FN is the input csv file name to read data from", default='WorldPorts.csv')
    parser.add_argument('-d', type=int, help="D is the number of data records to be loaded from the input csv file", default=10)
    parser.add_argument('-fr', type=int, help="FR is the number of the stored successor failure recovery. If not set no failure recovery action will be used")
    parser.add_argument('-fs', type=str, help="FS is the name of the file in which statistics will be written", default='statistics.csv')
    args = parser.parse_args()
    if args.fr: return args.n, args.d, args.fn, args.fs, True, args.fr
    else: return args.n, args.d, args.fn, args.fs, False, 0

#main procedure of the program

if __name__ == "__main__":
    global print_finger_table
    global next_available_port
    global pc_ip
    global AllNodes
    global failure_recovery
    global n
    global input_file_name
    global statistics_file_name
    global data_records_to_be_read
    global init_records
    global r
    global stat_id
    print("---------------^--------------------------------^---------------------------------------------")
    print('|CHORD - SIMULATION')
    print('|Developed by: Ioannis Chatzimichalis Eirini Rouchota Christina Papastavrou Christos Georgiadis')
    print('|CEID-Feb 2022')
    print("---------------ν--------------------------------ν---------------------------------------------")
    #CheckArgs(sys.argv)
    n, data_records_to_be_read, input_file_name, statistics_file_name, failure_recovery, r = define_args()
    print(f"Initial settings :\n\t n={n},d={data_records_to_be_read},fn={input_file_name},fs={statistics_file_name},fr={failure_recovery}, r={r}")
    input_file = is_a_valid_csv_file_name(input_file_name)
    if (input_file is None):
        print(f"Invalid initial data file name...")
        exit(-4)
    stat_id = datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(" ", "_").replace("/", "_").replace(":", "_")

    print(f"Creating initial configuration of the Chord ring", end=".")
    start = time.process_time_ns()
    pc_ip = socket.gethostbyname(socket.gethostname())
    next_available_port = initial_port
    AllNodes = dict()

    #create initial node
    node0_address = InetSocketAddress(pc_ip, next_available_port)
    next_available_port += 1
    new_node0 = Node(node0_address, r, True)
    AllNodes[node0_address.to_string()] = new_node0
    new_node0.activate()

    #create all the others nodes of the initial Chord ring congiguration
    for i in range(1, n):
        AddNewNode(True)
        display_progress("Creating initial configuration of the Chord ring", i, n)

    end1 = time.process_time_ns()
    print(f"\n\tInitial configuration of the Chord ring has been created in {elapsed(end1, start)} seconds.")
    start = time.process_time_ns()
    load_file_on_the_ring(new_node0, data_records_to_be_read)
    end2 = time.process_time_ns()
    print(f"\n\tInitial data contains ({init_records} records) : {elapsed(end2, start)} seconds to load.")
    if failure_recovery : fillSuccessorsList()

    #iterate user's selection until exit (0) is given
    user_selection = 1
    while user_selection != 0:
        user_selection = printMenuOptions()
        if user_selection not in (0, -1):
            processSelection(user_selection, new_node0)
            print("Enter to continue :", end='')
            input("")
        else:
            os.kill(os.getpid(), signal.SIGTERM)
