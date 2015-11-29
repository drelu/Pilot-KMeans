"""
    To execute: 
        /usr/hdp/2.3.2.0-2950/flink-0.10.1/bin/start-local.sh
        /usr/hdp/2.3.2.0-2950/flink-0.10.1/bin/pyflink2.sh flink.py
        
    see https://github.com/wdm0006/flink-python-examples
"""
from MDAnalysis.core.distances import distance_array, self_distance_array
from MDAnalysis.analysis.distances import contact_matrix
import scipy.sparse
from scipy.spatial.distance import cdist
import numpy as np
import time, os, sys, gc
import datetime
import logging
from flink.plan.Environment import get_environment
from flink.plan.Constants import INT, STRING
from flink.functions.GroupReduceFunction import GroupReduceFunction


FLINK_HOME="/usr/hdp/2.3.2.0-2950/flink-0.10.0/resources"
files=["../md_centered.xtc_95Atoms.np_txt", 
       "../md_prod_12x12_everymicroS_pbcmolcenter.xtc_44784Atoms.np_txt", 
       "../vesicle_1_5M_373_stride1000.xtc_145746Atoms.np_txt"]

NUMBER_EXECUTORS_SCENARIOS=[12]

env  = get_environment()
env.set_degree_of_parallelism(3)

#values = env.from_elements("Foo", "bar", "foobar", "fubar")
#values.map(lambda x: x, STRING).output()
#env.execute() 
#cutoff = 15.0



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
    return edge_list


def compute_distance(point_index):
    # 1-D Partitioning
    global coord_broadcast
    coord_all = coord_broadcast.value
    coord_part = coord_all[point_index-1:point_index]
    adj = (cdist(coord_part, coord_all) < cutoff)
    edge_list = get_edges_point(point_index, adj)
    del adj
    return edge_list


coord = np.loadtxt(files[0], dtype='float32')
coord_str=[]
for i in range(len(coord)):
    coord_str.append(str(coord[i][0]) +","+ str(coord[i][1]) +","+ str(coord[i][2]))


coord_ds = env.from_elements(tuple(coord_str))          
coord_ds.map(lambda x: 1, INT).output()

env.execute() 