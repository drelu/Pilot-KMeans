'''
Created on Jul 4, 2014

@author: Andre Luckow
'''

import os, sys, time
import logging
logger = logging.getLogger('MAPPER')
import redis
import threading
import numpy as np
import itertools
#import pilot
from distributed_inmem.dataunit import DistributedInMemoryDataUnit
from pilot import PilotComputeService, PilotCompute, ComputeUnit, State

class KMeans(object):

    @staticmethod
    def closestPoint(points, centers):
        bestIndex = 0
        closest = float("+inf")
        points = np.array([float(x) for x in points.split(",")])
        centers = np.array([[float(c.split(',')[0]), float(c.split(',')[1])] for c in centers])
        print "Centers: " + str(centers)
        for i in range(len(centers)):
            #dist = sum([(m-k)**2 for k,m in zip(points,centers[i]) ])
            dist = np.sum((points - centers[i]) ** 2)
            if dist < closest:
                closest = dist
                bestIndex = i
                print "Map point " + str(points) + " to index " + str(bestIndex)
        return (bestIndex, points.tolist())
    
    @staticmethod
    def averagePoints(points):
        points_extracted = [eval(i)[1] for i in points]
        points_np = np.array(points_extracted)
        new_center = np.mean(points_np, axis=0)
        return new_center 
    
    
def start_pilot(pilot_compute_description=None):
    COORDINATION_URL = "redis://localhost:6379"
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)
    if pilot_compute_description==None:
        pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 2,                             
                             "working_directory": os.getcwd() + "/work/",
                             }    
    pilot = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    return pilot

    

###################################################################################################
if __name__ == '__main__':
    
    DistributedInMemoryDataUnit.flushdb()
    
    f = open("data_20points.csv")
    points = f.readlines()
    f.close()
    
    pilot=start_pilot()
    du_points = DistributedInMemoryDataUnit("Points", pilot=pilot)
    du_points.load(points)
    
    f = open("centers.csv")
    centers = f.readlines()
    f.close()
    du_centers = DistributedInMemoryDataUnit("Centers")
    du_centers.load(centers)
        
    for iteration in range(0,5):
        
        output_dus = du_points.map_pilot("KMeans.closestPoint", du_centers.name)
        
        new_centers = []
        for du in output_dus:
            result_du=du.reduce_pilot("KMeans.averagePoints")
            new_centers.append(result_du)
            print result_du.name + ": " + str(result_du.export())
    
        #new_centers_string = ["%f,%f"%(c[0],c[1]) for c in new_centers]
        #du_centers.reload(new_centers_string)
    
    
    