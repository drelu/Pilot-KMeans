#  /home/01131/tg804093/src/flink-1.2.0/bin/pyflink2.sh $HOME/notebooks/Pilot-Memory/examples/jupyter/flink/flink_throughput.py

import sys, os, time, subprocess, datetime

run_timestamp=datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

scenarios = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072]
#scenarios = [16]    
for n in scenarios:
    cmd = "/home/01131/tg804093/src/flink-1.2.0/bin/pyflink2.sh $HOME/notebooks/Pilot-Memory/examples/jupyter/flink/flink_throughput_flink.py - " + str(n) + " " + run_timestamp
    print cmd
    os.system(cmd)
         
            
         
