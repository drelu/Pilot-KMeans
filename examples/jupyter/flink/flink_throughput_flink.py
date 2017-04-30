from flink.plan.Environment import get_environment
from flink.functions.GroupReduceFunction import GroupReduceFunction
import subprocess, time, sys, os, datetime

n = int(sys.argv[1])
run_timestamp  = sys.argv[2]

env = get_environment()
data = env.generate_sequence(1, n)
filename=run_timestamp + ".csv"

with open(filename, "a") as f:
    start_time = time.time()            
    data.map(lambda a: subprocess.check_output(["/bin/date"])).write_text("file:///gpfs/flash/users/tg804093/flink-out-%s-%s"%(run_timestamp, n))
    env.set_parallelism(1)
    env.execute(local=True)
    end_time= time.time()
    print("Flink-1.2.0, %d, Runtime, %.4f"%(n, (end_time-start_time)))
    f.write("Flink-1.2.0, %d, Runtime, %.4f\n"%(n, (end_time-start_time)))
    f.flush()