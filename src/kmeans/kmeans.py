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
from inmem.dataunit import DistributedInMemoryDataUnit
from pilot import PilotComputeService, PilotCompute, ComputeUnit, State

class KMeans(object):

    @staticmethod
    def closestPoint(points, centers):
        bestIndex = 0
        closest = float("+inf")
        points = np.array([float(x) for x in points.split(",")])
        centers = np.array([[float(c.split(',')[0]), float(c.split(',')[1])] for c in centers])
        for i in range(len(centers)):
            #dist = sum([(m-k)**2 for k,m in zip(points,centers[i]) ])
            dist = np.sum((points - centers[i]) ** 2)
            if dist < closest:
                closest = dist
                bestIndex = i
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
    
    f = open("data_1000points.csv")
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
        
        #best = du_points.map(KMeans.closestPoint, du_centers, 0, len(points))
        best = du_points.map_pilot(KMeans.closestPoint, du_centers)
        # sort points after centroid
        best.sort(key=lambda tup: tup[0])
        
        dus = {}
        for key, group in itertools.groupby(best, lambda x: x[0]):
            partition = "part-"+str(key)
            if not dus.has_key(partition):
                dus[partition] = DistributedInMemoryDataUnit(name=partition)
            dus[partition].load(group)    
        
        new_centers = []
        for key in sorted(dus):
            du = dus[key]
            new_center=du.reduce(KMeans.averagePoints, None)
            new_centers.append(new_center)
            print du.name + ": " + str(new_center)
    
        new_centers_string = ["%f,%f"%(c[0],c[1]) for c in new_centers]
        du_centers.reload(new_centers_string)
    
    
    