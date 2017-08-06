### Kafka Producer Multiprocess

from pykafka import KafkaClient
import numpy as np
import os, sys
import time
import datetime
sys.path.append("..")
import saga_hadoop_utils
from pykafka.partitioners import hashing_partitioner
import dask.array as da
import dask.bag as db
from distributed import Client


KAFKA_HOME="/home/01131/tg804093/work/kafka_2.11-0.10.1.0"
NUMBER_CLUSTER=[100]
NUMBER_POINTS_PER_CLUSTER=[10000]
NUMBER_DIM=3 # 1 Point == ~62 Bytes
NUMBER_POINTS_PER_MESSAGE=[5000] # 3-D Point == 304 KB
#NUMBER_POINTS_PER_MESSAGE=[10000] # 3-D Point == 304 KB
INTERVAL=0
NUMBER_OF_PRODUCES=1 # 10*60 = 10 minutes
NUMBER_PARTITIONS=48
TOPIC_NAME="Throughput"
NUMBER_PARALLEL_TASKS=8

zkKafka=saga_hadoop_utils.get_kafka_config_details(os.path.expanduser('~'))[1]
cmd="%s/bin/kafka-topics.sh --delete --zookeeper %s --topic %s"%(KAFKA_HOME, zkKafka, TOPIC_NAME)
print cmd
os.system(cmd)

cmd="%s/bin/kafka-topics.sh --create --zookeeper %s --replication-factor 1 --partitions %d --topic %s"%(KAFKA_HOME, zkKafka, NUMBER_PARTITIONS, TOPIC_NAME)
print cmd
os.system(cmd)

cmd="%s/bin/kafka-topics.sh --describe --zookeeper %s --topic %s"%(KAFKA_HOME, zkKafka, TOPIC_NAME)
print cmd
os.system(cmd)


#client = KafkaClient(hosts='c251-142.wrangler.tacc.utexas.edu:9092')
#consumer = topic.get_simple_consumer()

run_timestamp=datetime.datetime.now()
RESULT_FILE= "results/kafka-throughput-producer-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"

try:
    os.makedirs("results")
except:
    pass

output_file=open(RESULT_FILE, "w")
output_file.write("Number_Clusters,Number_Points_per_Cluster,Number_Dim,Number_Points_per_Message,Interval,Number_Partitions,Number_Processes,Time\n")

def get_random_cluster_points(number_points, number_dim):
    mu = np.random.randn()
    sigma = np.random.randn()
    p = sigma * np.random.randn(number_points, number_dim) + mu
    return p


#############################################################
# Init Dask
client = Client()        

def produce_block(db, num_cluster, num_point_per_cluster, num_points_per_message):
    global num_messages
    global bytes  
    global count
    
    print "Produce block: " + str(db)
    print "Number Cluster: " + str(num_cluster)

    # init Kafka
    client = KafkaClient(zookeeper_hosts=zkKafka)
    topic = client.topics[TOPIC_NAME]
    producer = topic.get_sync_producer(partitioner=hashing_partitioner)

    
    # partition on number clusters
    num_cluster_partition = num_cluster/NUMBER_PARALLEL_TASKS
    
    points = []
    for i in range(num_cluster_partition):    
        p = get_random_cluster_points(num_point_per_cluster, NUMBER_DIM)
        points.append(p)
    points_np=np.concatenate(points)
    print points_np.shape
    number_batches = points_np.shape[0]/num_points_per_message
    print "Points Array Shape: %s, Number Batches: %.1f"%(points_np.shape, number_batches)
    last_index=0
    for i in range(number_batches):
        print "Produce Batch: %d - %d, Num Messages: %d, Number Points per Message: %d, KBytes Transfered: %.1f, KBytes/sec: %s"%\
                                     (last_index,                                                                                           
                                      last_index+num_points_per_message, 
                                      num_messages,
                                      num_points_per_message,
                                      bytes/1024,
                                      bytes/1024/(time.time()-global_start))
        points_batch = points_np[last_index:last_index+num_points_per_message]
        points_strlist=str(points_batch.tolist())
        producer.produce(points_strlist, partition_key='{}'.format(count))
        count = count + 1
        last_index = last_index + num_points_per_message
        bytes = bytes + len(points_strlist)
        num_messages = num_messages + 1


        
global bytes        
bytes = 0
global num_messages
num_messages = 0
global count
count = 0
global_start = time.time()
for num_points_per_message in NUMBER_POINTS_PER_MESSAGE:
    for idx, num_cluster in enumerate(NUMBER_CLUSTER):
        count_produces = 0
        num_point_per_cluster = NUMBER_POINTS_PER_CLUSTER[idx]
        while count_produces < NUMBER_OF_PRODUCES:
            start = time.time()
            bag = db.from_sequence([str(x) for x in range(NUMBER_PARALLEL_TASKS)],  npartitions=NUMBER_PARALLEL_TASKS)
            bag.starmap(produce_block, num_cluster=num_cluster, 
                                num_point_per_cluster=num_point_per_cluster, 
                                num_points_per_message=num_points_per_message).compute()
            print "End Produce via Dask"
             
            end = time.time()
            print "Number: %d, Number Processes: %d, Time to produce %d points: %.1f"%(count_produces, NUMBER_PARALLEL_TASKS, num_cluster*num_point_per_cluster, end-start)
            output_file.write("%d,%d,%d,%d,%d,%d,%d,%.5f\n"%(num_cluster,num_point_per_cluster,NUMBER_DIM, 
                                                       num_points_per_message,INTERVAL,NUMBER_PARTITIONS,NUMBER_PARALLEL_TASKS, (end-start)))
            output_file.flush()
            count_produces = count_produces + 1
            #time.sleep(INTERVAL)
    
        #time.sleep(INTERVAL)

output_file.close()
