import cmd
import os
import sys
from multiprocessing.connection import Client

data_dir = 'data'

setup_file = 'cli_setup.txt'
if len(sys.argv) > 1:
    setup_file = sys.argv[1]
setup = open(setup_file)
mappers = []

#First line is replicator address, next is reducer address
ip, port = setup.readline().strip().split()
replicator = Client((ip, int(port)))
ip, port = setup.readline().strip().split()
reducer = Client((ip, int(port)))

#Next set up mappers
for _ in range(int(setup.readline())):
    ip, port = setup.readline().strip().split()
    mappers.append(Client((ip, int(port))))

class MapReduceCLI(cmd.Cmd):
    intro = "Type help or ? to list commands.\n"
    prompt = "(mapreduce) "

    def do_map(self, arg):
        'Splits input file into equal parts and sends filename to each mapper with respective offsets'
        f = open(data_dir + '/' + arg)
        #Find size of file and split size
        size = f.seek(0, os.SEEK_END)
        split_size = size // len(mappers)
        #Send filename and offsets to respective mappers
        for i, mapper in enumerate(mappers):
            mapper.send((arg, split_size*i, split_size*(i+1)))
            if mapper.recv() != "OK":
                print("Error in the mapper")
            else:
                print("Map successful")
        f.close()
        
    def do_reduce(self, arg):
        'Takes intermediate files and sends the filenames to the reducer'
        reducer.send(arg)
        if reducer.recv() != "OK":
            print("Error in the reducer")
        else:
            print("Reduce successful")

    def do_replicate(self, arg):
        'Sends a message to the PRM to replicate the file with the other nodes'
        replicator.send('replicate' + ' ' + arg)
        if replicator.recv() != "OK":
            print("Error in replicate")
        else:
            print("Replicate successful")

    def do_stop(self, arg):
        'Sends a message to the PRM telling it to stop'
        replicator.send('stop')

    def do_resume(self, arg):
        'Sends a message to the PRM telling it to resume'
        replicator.send('resume')

    def do_total(self, arg):
        'Asks PRM to print total number of words at positions in the log'
        replicator.send('total' + ' ' + arg)

    def do_print(self, arg):
        'Prints filenames of all log objects'
        replicator.send('print')

    def do_merge(self, arg):
        'Performs a reduce at the given positions in the log'
        replicator.send('merge' + ' ' + arg)

if __name__ == '__main__':
    MapReduceCLI().cmdloop()