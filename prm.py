import sys
import threading
import time
import select
from multiprocessing.connection import Listener, Client
from queue import Queue
from collections import Counter

cli_port, proposer_port, acceptor_port, my_id, detached = map(int, sys.argv[1:])
#Command line communicates through this listener
cli_listener = Listener(('0.0.0.0', cli_port))
reduce_output_dir = 'reduce_output'

#Sets up "from" connections
proposer_listener = Listener(('0.0.0.0', proposer_port), backlog=5)
acceptor_listener = Listener(('0.0.0.0', acceptor_port), backlog=5)

#Connect to other replicators, save "to" channels
setup_file = open('prm_setup.txt')
N = int(setup_file.readline().strip())
to_proposers = [None] * N
to_acceptors = [None] * N
from_proposers = [None] * N
from_acceptors = [None] * N
for count, line in enumerate(setup_file):
    if count >= N:
        break
    print("Adding {}".format(count))
    ip, proposer_port, acceptor_port, i = line.strip().split()
    while True:
        try:
            to_proposers[int(i)] = Client((ip, int(proposer_port)))
            to_acceptors[int(i)] = Client((ip, int(acceptor_port)))
            break
        except:
            time.sleep(0.1)

#Accept connections and save as "from" channels
accepted_proposers = []
accepted_acceptors = []
for _ in to_proposers:
    accepted_proposers.append(acceptor_listener.accept())
    accepted_acceptors.append(proposer_listener.accept())

for sock1, sock2 in zip(to_proposers, to_acceptors):
    sock1.send(my_id)
    sock2.send(my_id)

for sock in accepted_proposers:
    _id = sock.recv()
    from_proposers[_id] = sock
for sock in accepted_acceptors:
    _id = sock.recv()
    from_acceptors[_id] = sock
        
#We don't move past this point until all replicators are up

state = 'resume'
log = {}

ballot_num_proposer = (0,0)
ballot_num_acceptor = (0,0)
accept_num = (0,0)
accept_val = None

def propose(filename, counts):
    global ballot_num_proposer
    my_val = (filename, counts)
    decided = False
    while not decided:
        print("Trying a round of proposing")
        ballot_num_proposer = (ballot_num_proposer[0] + 1, my_id)
        for acceptor in to_acceptors:
            print("Proposing to acceptor")
            acceptor.send(('prepare', ballot_num_proposer))
        replies = []
        for _id, acceptor in enumerate(from_acceptors):
            if acceptor.poll(0.25):
                print("Reading reply from {}".format(_id))
                reply, *args = acceptor.recv()
                if reply == 'ack':
                    print('Got ack from {}'.format(_id))
                    replies.append(args)
        print(replies)
        if len(replies) > len(from_acceptors) // 2:
            print("Got quorum")
            if any([accept_val for b, accept_num, accept_val in replies]):
                highest = max([(accept_num, accept_val) for b, accept_num, accept_val in replies if accept_val])
                if highest[0] < ballot_num_proposer:
                    best_val = my_val
                else:
                    best_val = highest[1]
            else:
                best_val = my_val
            for acceptor in to_acceptors:
                acceptor.send(('accept', ballot_num_proposer, best_val))
            replies = []
            for _id, acceptor in enumerate(from_acceptors):
                print('Did {} accept?'.format(_id))
                if acceptor.poll(0.25):
                    reply, *args = acceptor.recv()
                    if reply == 'accept':
                        replies.append(args)
            if len(replies) > len(from_acceptors) // 2:
                print('Deciding on {}'.format(best_val))
                if best_val == my_val:
                    decided = True
                for acceptor in to_acceptors:
                    acceptor.send(('decide', best_val))

def acceptor_thread():
    global ballot_num_acceptor
    global accept_num
    global accept_val
    while True:
        if state == 'stop':
            for proposer, acceptor in zip(from_proposers, from_acceptors):
                #Drop incoming messages
                while proposer.poll():
                    _ = proposer.recv()
                while acceptor.poll():
                    _ = acceptor.recv()
            time.sleep(0.1)
            continue
        for _id, proposer in enumerate(from_proposers):
            while proposer.poll(0.01):
                print('Reading from {}'.format(_id))
                message, *args = proposer.recv()
                if message == 'prepare':
                    print("Prepare from {}".format(_id))
                    if args[0] > ballot_num_acceptor:
                        print("Sending {} an ack".format(_id))
                        ballot_num_acceptor = args[0]
                        to_proposers[_id].send(('ack', args[0], accept_num, accept_val))
                    else:
                        to_proposers[_id].send(('nack'))
                elif message == 'accept':
                    print("Accept from {}".format(_id))
                    if args[0] >= ballot_num_acceptor:
                        ballot_num_acceptor = args[0]
                        accept_num = args[0]
                        accept_val = args[1]
                        to_proposers[_id].send(('accept', accept_num, accept_val))
                    else:
                        to_proposers[_id].send(('nack'))
                else:
                    print("Decide from {}".format(_id))
                    log[int(ballot_num_acceptor[0])] = args[0]
                
my_acceptor = threading.Thread(target=acceptor_thread)
my_acceptor.start()

#The current thread listens for commands from CLI.
if not int(detached):
    conn = cli_listener.accept()
    print("Connected to CLI")
    while True:
        command, *args = conn.recv().split()
        if command == 'stop':
            state = 'stop'
        if command == 'resume':
            state = 'resume'
        if command == 'replicate':
            if state == 'resume':
                for arg in args:
                    #Convert file to counter object
                    f = open(reduce_output_dir + '/' + arg)
                    counts = Counter()
                    for line in f:
                        if len(line.strip().split()) < 2:
                            break
                        key, val = line.strip().split()
                        counts[key] = int(val)
                    #Run proposer
                    propose(arg, counts)
            conn.send('OK')
        if command == 'total':
            print("Received 'total' command:")
            print(sum(sum([counter for key, (filename, counter) in log.items() if key in map(int, args)], Counter()).values()))
        if command == 'print':
            print("Received 'print' command:")
            for key in log:
                print(log[key][0])
        if command == 'merge':
            print("Received 'merge' command:")
            print(sum([counter for key, (filename, counter) in log.items() if key in map(int, args)], Counter()))
