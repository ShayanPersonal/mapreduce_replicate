# Map-reduce replicate algorithm in Python.

- /setup.py: Runs the mappers, reducer, and prm in the background.
- /cli_setup.txt: Used by cli.py. First line is address of local prm. Next line is address of local reducer. The following N - lines are address of the local mappers.
- /prm_setup.txt: Used by prm.py. First line is # of nodes N participating. Next N lines are (ip, proposer_port, acceptor_port, id)
- /data/: Directory where text data is stored.
- /map_output/: Directory where mapped files are stored.
- /reduce_output/: Directory where reduced files are stored.

You don't need to use the full file path in the CLI. Just give it the name of the file (remember to put it in /data/)

After running setup.py, run cli.py in a new shell. This should bring you to the mapreduce command line. Check the shell running setup.py for any errors.

You can run any of the commands from the project. If running merge, total, or print, check the shell running setup.py for the output.

mapper.py and reducer.py will automatically be ran by setup.py.

Example usage:
1. Open up 2 shells.
2. In shell 1: 
	python3 setup.py
3. In shell 2
	python3 cli.py
	(mapreduce) map words1
	(mapreduce) reduce words1_I_1 words1_I_2
	(mapreduce) replicate words1_reduced
4. Go back to shell 1 to see mapreduce output.

If there's errors setup.py may not exit the processes sometimes. You may have to kill them manually to free up the ports.
