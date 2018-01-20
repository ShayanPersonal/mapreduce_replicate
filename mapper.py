import sys
from multiprocessing.connection import Listener
from collections import Counter

port, my_id = map(int, sys.argv[1:])
server = Listener(('0.0.0.0', port))
data_dir = 'data'

def my_identity(data):
    #Identity function for testing
    return data

def my_counter(data):
    #Gets counts of words in data.
    counts = Counter(data.strip().split())
    transformed = ''
    for key, val in counts.items():
        transformed += '{} {}\n'.format(key, val)
    return transformed[:-1]

def my_map(data):
    return my_counter(data)

conn = server.accept()
while True:
    #Receive filename and offsets
    filename, start, end = conn.recv()
    f = open(data_dir + '/' + filename)
    #Don't start reading from the middle of a word.
    if start != 0:
        f.seek(start-1)
        while not f.read(1).isspace():
            start += 1
    data = f.read(end - start)
    #Don't end reading in the middle of a word.
    char = f.read(1)
    if not data[-1].isspace():
        while char and not char.isspace():
            data += char
            char = f.read(1)
    #Run map function on the data and print it to intermediate file
    transformed = my_map(data)
    print(transformed, file=open('map_output/{}_I_{}'.format(filename.split('/')[-1], my_id), 'w'))
    conn.send("OK")
    f.close()
