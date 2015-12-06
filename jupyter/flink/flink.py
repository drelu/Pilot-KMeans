from __future__ import print_function
"""
    To execute: 
        
        Local:
        
            /usr/hdp/2.3.2.0-2950/flink-0.10.1/bin/start-local.sh
        
        YARN:
            
            yarn-session.sh -s 24 -tm 90000 -n 4
        
        
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
from flink.plan.Constants import INT, STRING, WriteMode
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.MapFunction import MapFunction

FLINK_HOME="/usr/hdp/2.3.2.0-2950/flink-0.10.0/resources"
files=["/data/leafletfinder/small/md_centered.xtc_95Atoms.np_txt", 
       "/data/leafletfinder/medium/md_prod_12x12_everymicroS_pbcmolcenter.xtc_44784Atoms.np_txt", 
       "/data/leafletfinder/large/vesicle_1_5M_373_stride1000.xtc_145746Atoms.np_txt"]

NUMBER_EXECUTORS_SCENARIOS=[12]


from numpy import genfromtxt


#values = env.from_elements("Foo", "bar", "foobar", "fubar")
#values.map(lambda x: x, STRING).output()
#env.execute() 

cutoff = 15.0

class MapperBcv(MapFunction):
    
    cutoff = 15.0

    def get_edges_point(self, point_index, adjacency_matrix, cutoff=15.0):
        edge_list = []
        for index, i in np.ndenumerate(adjacency_matrix):
            #print ("Index: %d, Value: %d"%(index[i], i))
            #if point_index<=index[1] and i<cutoff:
            if i==True and point_index<=index[1]:
                # Attention we only compute the upper half of the adjacency matrix
                # thus we need to offset the target edge vertice by point_index
                edge_list.append((point_index, point_index+index[1]))
        return edge_list

    
    def convert_str_to_np(self, coord_string):
        coord_data=[]
        for i in coord_string:
            coord_data.append(i.split(","))
        return np.array(coord_data, dtype='float32')    
    
    
    def map(self, value):
        point_index = value
        coord_all = self.convert_str_to_np(self.context.get_broadcast_variable("bcv"))
        coord_part = coord_all[point_index-1:point_index]
        adj = (cdist(coord_part, coord_all) < cutoff)
        edge_list = self.get_edges_point(point_index, adj)
        del adj
        return str(edge_list)
        
        
################################################################################
# Process one point at a time

def compute_distance(point_index):
    # 1-D Partitioning
    global coord_broadcast
    coord_all = coord_broadcast.value
    coord_part = coord_all[point_index-1:point_index]
    adj = (cdist(coord_part, coord_all) < cutoff)
    edge_list = get_edges_point(point_index, adj)
    del adj
    return edge_list


if __name__ == "__main__":  

    env  = get_environment()
    env.set_degree_of_parallelism(16)

    data_file=files[2]
    coord = np.loadtxt(data_file, dtype='float32')
    coord_str=[]
    for i in range(len(coord)):
        coord_str.append(str(coord[i][0]) +","+ str(coord[i][1]) +","+ str(coord[i][2]))

    coord_ds = env.from_elements(*(range(len(coord))))
    #coord_ds.map(lambda x: 1, INT).write_text("out.txt", write_mode=WriteMode.OVERWRITE)
    toBroadcast = env.from_elements(*(coord_str)) 
    coord_ds.map(MapperBcv(), STRING).\
             with_broadcast_set("bcv", toBroadcast).\
             write_text("out.txt", write_mode=WriteMode.OVERWRITE)

    start = time.time()
    env.execute() 
    print("ComputeDistanceFlink, %.2f"%(time.time()-start))
    
