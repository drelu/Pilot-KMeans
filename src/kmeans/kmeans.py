'''
Distributed In-Memory KMeans
Created on Jul 4, 2014

@author: Andre Luckow
'''

import os, sys, time
import logging
logger = logging.getLogger('DistributedInMemoryDataUnit-KMeans')
logger.setLevel(logging.DEBUG)
import redis
import threading
import numpy as np
import itertools
import datetime
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
        logger.debug("**closestPoint - Point: " + str(points) + " Centers: " + str(centers))
        for i in range(len(centers)):
            #dist = sum([(m-k)**2 for k,m in zip(points,centers[i]) ])
            dist = np.sum((points - centers[i]) ** 2)
            if dist < closest:
                closest = dist
                bestIndex = i
                logger.debug("Map point " + str(points) + " to index " + str(bestIndex))
        return (bestIndex, points.tolist())
    
    @staticmethod
    def averagePoints(points):
        logger.debug("Call average points on: " + str(points))
        points_extracted = [eval(i)[1] for i in points]
        points_np = np.array(points_extracted)
        new_center = np.mean(points_np, axis=0)
        logger.debug("New center: " + str(new_center))
        new_center_string = ','.join(['%.5f' % num for num in new_center])
        logger.debug("New center string: " + new_center_string) 
        return new_center_string 
    
    
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


PERFORMANCE_DATA_FILE="DIDU-kmeans-results-" 
FIELDS=["NumberPoints", "Pilot", "KMeansImpl", "RunTimestamp", "Type"]
RESULT_DIR="results"
NUM_ITERATIONS=2

###################################################################################################
if __name__ == '__main__':
    
    DistributedInMemoryDataUnit.flushdb()
    run_timestamp=datetime.datetime.now()
    time_measures={}
    
    #############################################################################
    start = time.time()
    pilot_compute_description = {
                             "service_url": 'fork://localhost',
                             "number_of_processes": 2,                             
                             "working_directory": os.getcwd() + "/work/",
                             }   
    pilot=start_pilot()
    end_start_pilot = time.time()
    time_measures["Pilot Submission"]=end_start_pilot-start
    logger.debug("Started pilot in %.2f sec"%time_measures["Pilot Submission"])
    #############################################################################
    
    
    logger.debug("Start KMeans")
    f = open("data_20points.csv")
    points = f.readlines()
    f.close()
    number_of_data_points=len(points)    
    du_points = DistributedInMemoryDataUnit("Points", pilot=pilot)
    du_points.load(points)
    
    f = open("centers.csv")
    centers = f.readlines()
    f.close()
    du_centers = DistributedInMemoryDataUnit("Centers")
    du_centers.load(centers)
    number_of_centroids_points=len(centers)  

    end_data_load = time.time()
    time_measures["DataLoadTime"] = end_data_load-end_start_pilot
        
    for iteration in range(0,NUM_ITERATIONS):
        iteration_start = time.time()
        output_dus = du_points.map_pilot("KMeans.closestPoint", du_centers.name)
        
        new_centers = []
        for du in output_dus:
            result_du=du.reduce_pilot("KMeans.averagePoints")
            new_centers.append(result_du)
                    
        du_centers = DistributedInMemoryDataUnit("Centers-%d"%(iteration+1)).merge(new_centers)
        iteration_end = time.time()
        time_measures["Iteration-%d"%iteration] = iteration_end - iteration_start
             
    end = time.time()
    time_measures["Runtime"] = end-start
            
    #################################################################################################################
    # print output        
    try:
        os.mkdir(RESULT_DIR)
    except:
        pass
    
    output_data = open(os.path.join(RESULT_DIR, PERFORMANCE_DATA_FILE + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"), "w")
    header=",".join(["KMeansImpl", "NumberPoints", "NumberCentroids", "NumberIterations" "Pilot", "NumberCores" "RunTimestamp", "Type", "Value"])
    line = ("DIDU-KMeans", str(number_of_data_points), str(number_of_centroids_points), 
            str(NUM_ITERATIONS), pilot_compute_description["service_url"], 
            str(pilot_compute_description["number_of_processes"]), run_timestamp.isoformat())
    
    for time_type, value in time_measures.items():
        print_string=",".join(line + (time_type, str(value)))
        output_data.write(print_string + "\n")
        
    output_data.close()
    
    
    
    