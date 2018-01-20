import subprocess
import time
a = subprocess.Popen("python3 mapper.py 5011 1".split())
b = subprocess.Popen("python3 mapper.py 5012 2".split())
c = subprocess.Popen("python3 reducer.py 5013".split())
d = subprocess.Popen("python3 prm.py 5014 5015 5016 0 0".split()) #cli_port, proposer_port, acceptor_port, id, detached_from_cli
#e = subprocess.Popen("python3 prm.py 5017 5018 5019 1 1".split())
#f = subprocess.Popen("python3 prm.py 5020 5021 5022 2 1".split())
while not (a.poll() or b.poll() or c.poll() or d.poll()):
    time.sleep(1)
a.kill()
b.kill()
c.kill()
d.kill()
#e.kill()
#f.kill()
