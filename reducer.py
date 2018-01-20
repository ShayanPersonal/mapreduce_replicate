import sys
from multiprocessing.connection import Listener
from collections import Counter

port = int(sys.argv[1])
server = Listener(('0.0.0.0', port))
map_output_dir = 'map_output'

def my_count_reduce(files):
    final_counter = Counter()
    for filename in files:
        f = open(map_output_dir + '/' + filename)
        for line in f:
            key, val = line.strip().split()
            final_counter[key] += int(val)
        f.close()
    reduced = ''
    for key, val in final_counter.items():
        reduced += '{} {}\n'.format(key, val)
    return reduced

def my_reduce(files):
    return my_count_reduce(files)

conn = server.accept()
while True:
    #Receive filename and start and end offsets
    files = conn.recv().strip().split()
    #Run reduce on all the files and print to output file
    reduced = my_reduce(files)
    print(reduced, file=open('reduce_output/{}_reduced'.format(files[0].split('_')[0]), 'w'))
    conn.send("OK")
