from MDAnalysis.core.distances import distance_array, self_distance_array
from MDAnalysis.analysis.distances import contact_matrix
import numpy as np
import time, os, sys, gc
from pilot_hadoop import PilotComputeService as PilotSparkComputeService

execfile("../util/init_spark.py")

files=["../md_centered.xtc_95Atoms.np_txt", 
       "../md_prod_12x12_everymicroS_pbcmolcenter.xtc_44784Atoms.np_txt", 
       "../vesicle_1_5M_373_stride1000.xtc_145746Atoms.np_txt"]


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
    print "ComputeDistanceSpark, %d, %d, %.2f"%(len(coord_all), NUMBER_EXECUTORS, (time.time()-start))
    pilot_spark.cancel()

def benchmark_mdanalysis()
    start = time.time()
    #distance_array(coord, coord, box=None)
    contact_matrix(coord, returntype="sparse")
    print "ComputeDistanceMDAnalysis, %d, %.2f"%(len(coord), (time.time()-start))
     
        
results=[]
for f in files:
    print f
    coord = np.loadtxt(f, dtype='float32')
    benchmark_spark(coord, 48)
    del coord
    gc.collect()
    
    
