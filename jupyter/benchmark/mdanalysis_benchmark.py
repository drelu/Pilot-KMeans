from MDAnalysis.core.distances import distance_array, self_distance_array
from MDAnalysis.analysis.distances import contact_matrix
import scipy.sparse
from scipy.spatial.distance import cdist
import numpy as np
import time, os, sys, gc
import datetime
import tensorflow as tf
import logging
logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)

#execfile("../util/init_spark.py")
#import pyspark.mllib.linalg.distributed
#from pilot_hadoop import PilotComputeService as PilotSparkComputeService

RESULT_DIR="results"
RESULT_FILE_PREFIX="mdanalysis-distance-spark-"
HEADER_CSV="Scenario, NumberAtoms, NumberExecutors, Time"

#files=["../md_centered.xtc_95Atoms.np_txt", 
#       "../md_prod_12x12_everymicroS_pbcmolcenter.xtc_44784Atoms.np_txt", 
#       "../vesicle_1_5M_373_stride1000.xtc_145746Atoms.np_txt"]

DATA_PATH="/data/leafletfinder/synthetic/"
DATA_PATH="../data/mdanalysis/synthetic/traj"
files=[os.path.join(DATA_PATH, i) for i in os.listdir(DATA_PATH)]

NUMBER_EXECUTORS_SCENARIOS=[1]

global coord_broadcast
global cutoff

cutoff = 15.0

################################################################################
# Process one point at a time
def get_edges_point(point_index, adjacency_matrix, cutoff=15.0):
    edge_list = []
    for index, i in np.ndenumerate(adjacency_matrix):
        #print ("Index: %d, Value: %d"%(index[i], i))
        #if point_index<=index[1] and i<cutoff:
        if i==True and point_index<=index[1]:
            # Attention we only compute the upper half of the adjacency matrix
            # thus we need to offset the target edge vertice by point_index
            edge_list.append((point_index, point_index+index[1]))
    #del adjacency_matrix
    return edge_list


def compute_distance(point_index):
    # 1-D Partitioning
    global coord_broadcast
    coord_all = coord_broadcast.value
    coord_part = coord_all[point_index-1:point_index]
    #adj = (distance_array(coord_part, coord_all[point_index:], box=None) < cutoff)
    adj = (cdist(coord_part, coord_all) < cutoff)
    #adj = cdist(coord_part, coord_all)
    edge_list = get_edges_point(point_index, adj)
    del adj
    #del coord_part
    #del coord_all
    #gc.collect()
    return edge_list


def benchmark_spark(coord, NUMBER_EXECUTORS):
    global coord_broadcast
    print "******************* RUN WITH %d EXECUTORS ***********"%NUMBER_EXECUTORS
    pilotcompute_description = {
        "service_url": "yarn-client://yarn.radical-cybertools.org",
        "number_of_processes": NUMBER_EXECUTORS,
        "physical_memory_per_process": "3G" 
    }

    print "SPARK HOME: %s"%os.environ["SPARK_HOME"]
    print "PYTHONPATH: %s"%os.environ["PYTHONPATH"]

    start = time.time()
    pilot_spark = PilotSparkComputeService.create_pilot(pilotcompute_description=pilotcompute_description)
    sc = pilot_spark.get_spark_context()
    print "SparkStartup, %.2f"%(time.time()-start)
    coord_broadcast = sc.broadcast(coord)    
    part_rdd=sc.parallelize(range(len(coord_broadcast.value)), NUMBER_EXECUTORS)
    part_rdd.cache()
    start = time.time()
    edges_list=part_rdd.map(compute_distance).flatMap(lambda a: a).collect()
    result="ComputeDistanceSpark, %d, %d, %.2f"%(len(coord_broadcast.value), NUMBER_EXECUTORS, (time.time()-start))
    pilot_spark.cancel()
    return result


def benchmark_spark_cart(coord, NUMBER_EXECUTORS):
    print "******************* RUN WITH %d EXECUTORS ***********"%NUMBER_EXECUTORS
    pilotcompute_description = {
        "service_url": "yarn-client://yarn.radical-cybertools.org",
        "number_of_processes": NUMBER_EXECUTORS,
        "physical_memory_per_process": "3G" 
    }

    print "SPARK HOME: %s"%os.environ["SPARK_HOME"]
    print "PYTHONPATH: %s"%os.environ["PYTHONPATH"]

    start = time.time()
    pilot_spark = PilotSparkComputeService.create_pilot(pilotcompute_description=pilotcompute_description)
    sc = pilot_spark.get_spark_context()
    print "SparkStartup, %.2f"%(time.time()-start)
    coord_matrix=pyspark.mllib.linalg.distributed.RowMatrix(sc.parallelize(coord, NUMBER_EXECUTORS))
    row_rdd=coord_matrix.rows
    start = time.time()
    distances=  row_rdd.cartesian(row_rdd).\
                map(lambda a: (a[0].squared_distance(a[1]))).\
                filter(lambda a: a>15.0).\
                collect()
    print "ComputeDistance, %.2f"%(time.time()-start)



def benchmark_mdanalysis(coord, NUMBER_EXECUTORS=1):
    start = time.time()
    #distance_array(coord, coord, box=None)
    contact_matrix(coord, returntype="sparse")
    result="ComputeDistanceMDAnalysisSparse, %d, %.2f"%(len(coord), (time.time()-start))
    return result


def benchmark__tf(coord):
    start = time.time()
    sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))
    matrix1 = tf.convert_to_tensor(coord.astype("float32"))
    matrix_extend=tf.expand_dims(matrix1, 1)
    dist_matrix=tf.sub(matrix_extend, matrix1)
    dist_matrix_pow=tf.pow(dist_matrix, 2)
    dist_matrix_pow_red=tf.reduce_sum(dist_matrix_pow, 2)
    dist_matrix_euc=tf.sqrt(dist_matrix_pow_red)
    rc=sess.run(dist_matrix_euc)
    result="ComputeDistanceTensorflow, %d, %.2f"%(len(coord), (time.time()-start))
    sess.close()
    return result
       

if __name__ == "__main__":       
    try:
        os.mkdir(RESULT_DIR)
    except:
        pass        

    results=[]
    d =datetime.datetime.now()
    result_filename = RESULT_FILE_PREFIX + d.strftime("%Y%m%d-%H%M%S") + ".csv"
    f = open(os.path.join(RESULT_DIR, result_filename), "w")
    f.write(HEADER_CSV+ "\n")

    for i in range(10):
        for file_name in files:
            print "Process: " + file_name
            coord = np.loadtxt(file_name, dtype='float32')
            for i in NUMBER_EXECUTORS_SCENARIOS:
                #result=benchmark_spark(coord, i)
                result=benchmark_mdanalysis(coord, i)
                result=benchmark__tf(coord)
                results.append(result)
                f.write(result + "\n")
                f.flush()
            
            del coord
            gc.collect()

    f.close()
    print("Finished run")