'''
Distributed In-Memory KMeans

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
<<<<<<< HEAD
import pdb
from distributed_inmem.dataunit import DistributedInMemoryDataUnit, InMemoryCoordination
from pilot import PilotComputeService, PilotCompute, ComputeUnit, State
=======

try:
    from distributed_inmem.dataunit import DistributedInMemoryDataUnit
    from pilot import PilotComputeService, PilotCompute, ComputeUnit, State
except:
    print "Please install BigJob!"
>>>>>>> master

class KMeans(object):

    @staticmethod
    def closestPoint(points, centers):
        bestIndex = 0
        closest = float("+inf")
        print "Hello", points
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
    

PERFORMANCE_DATA_FILE="DIDU-kmeans-results-" 
NUM_ITERATIONS=5
RUNS=1

inputFiles = ["/scratch/01539/pmantha/input/1000000Points50Centers.csv", "/scratch/01539/pmantha/input/100000Points500Centers.csv", "/scratch/01539/pmantha/input/10000Points5000Centers.csv"]
clusters = [50,500,5000]
nbrMappers = [96, 192, 384]


PILOT_COORDINATION_URL = "redis://login3.stampede.tacc.utexas.edu:6380"
INMEM_COORDINATION_HOST = "login1.stampede.tacc.utexas.edu"



###################################################################################################
if __name__ == '__main__':

    for run in range(3):    
        output_data = open(os.path.join(PERFORMANCE_DATA_FILE + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"), "w")
        header=",".join(["Run","KMeansImpl", "File", "NumberDataPoints", "NumberCentroids", "RunTimestamp", "Mappers", "AverageMapTimePerIteration", "AverageReduceTimePerIteration"]) + "\n"                          
        output_data.write(header)


        try:            
            for m in  nbrMappers:                      
                for ex in range(len(inputFiles)):
                    time_measures={}

                    start_pilot = time.time()
                    #############################################################################
                    pilot_compute_description = { "service_url": 'slurm+ssh://stampede.tacc.xsede.org',
                                                   "working_directory": '/scratch/01539/pmantha/pilot-compute',
                                                   "queue":"normal",
                                                   "project":"TG-MCB090174" ,
                                                   "affinity_datacenter_label": 'eu-de-south-1',
                                                   "affinity_machine_label": 'mymachine-1',
                                                   "walltime":120,
                                                   "number_of_processes": m,
                                                 }  
                    pilot_compute_service = PilotComputeService(coordination_url=PILOT_COORDINATION_URL)                                         
                    pilot = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
                    end_start_pilot = time.time()
                    time_measures["Pilot Submission"]=end_start_pilot-start_pilot
                    logger.debug("Started pilot in %.2f sec"%time_measures["Pilot Submission"])
                    #############################################################################            
                                
                    df = InMemoryCoordination(flushdb=True, pilot=pilot, hostname=INMEM_COORDINATION_HOST)                        

                    start = time.time()
                    logger.debug("Start KMeans for input file %s"%(inputFiles[ex]))
                    f = open(inputFiles[ex])
                    points = f.readlines()
                    f.close()
                    number_of_data_points=len(points)    
                    du_points = DistributedInMemoryDataUnit(name="Points", coordination=df)
                    du_points.load(points)    
                    centers = points[:clusters[ex]]
        
                    du_centers = DistributedInMemoryDataUnit(name="Centers", coordination=df)
                    du_centers.load(centers)
                    number_of_centroids_points=len(centers)  

                    end_data_load = time.time()
                    time_measures["DataLoadTime"] = end_data_load-end_start_pilot
    
                    total_map_time = 0
                    total_reduce_time = 0
                    for iteration in range(0,NUM_ITERATIONS):
                        iteration_start = time.time() 
                        map_start_time = time.time() 
                        future = du_points.map_pilot("KMeans.closestPoint", du_centers.name, number_of_compute_units=m, number_of_cores_per_compute_unit=1)
                        output_dus = future.result()        
                        map_end_time = time.time()
                    
                        logger.debug("wait for all map objects.................%s "%(str(round(map_end_time-map_start_time, 2))))
                    
                        total_map_time = total_map_time + (map_end_time-map_start_time)
                    
                        new_centers = []
                        futureObjs = []
                        for du in output_dus:
                            futureObjs.append(du.reduce_pilot("KMeans.averagePoints", number_of_cores_per_compute_unit=1))

                        for f in futureObjs:                                
                            result_du = f.result()
                            new_centers.append(result_du)
                        reduce_end_time = time.time()
                        logger.debug("wait for all reduce objects................. %s" % (str(round(reduce_end_time-map_end_time, 2))))
                    
                        total_reduce_time = total_reduce_time + (reduce_end_time-map_end_time)
                
                        du_centers = DistributedInMemoryDataUnit(name="Centers-%d"%(iteration+1), coordination=df).merge(new_centers)
                        iteration_end = time.time()
                        time_measures["Iteration-%d"%iteration] = iteration_end - iteration_start
         
                    end = time.time()
                    time_measures["Runtime"] = end-start
                    time_measures["average_map"] = total_map_time/NUM_ITERATIONS
                    time_measures["average_reduce"] = total_reduce_time/NUM_ITERATIONS
                    du_points.delete()
                    du_centers.delete()                    
        
                    #################################################################################################################
    
                    line = ("DIDU-KMeans", str(inputFiles[ex]), str(number_of_data_points), str(number_of_centroids_points), str(time_measures["Runtime"]),
                            str(m), str(time_measures["average_map"]), str(time_measures["average_reduce"]))
                    output_data.write(",".join(line) + "\n")
                    output_data.flush()    
                    pilot_compute_service.cancel()                                        
        except Exception, e:     
            print "exeception: e ", e
            output_data.close()
            pilot_compute_service.cancel()
            sys.exit(0)
        
        output_data.close()
