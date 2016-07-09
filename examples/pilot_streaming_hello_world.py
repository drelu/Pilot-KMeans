import os, sys, time
import logging
logger = logging.getLogger('DistributedInMemoryDataUnit-KMeans')
logger.setLevel(logging.DEBUG)
import redis
import threading
import numpy as np
import itertools
import datetime


try:
    from distributed_inmem.dataunit_kafka import DistributedInMemoryDataUnit
    from pilot import PilotComputeService, PilotCompute, ComputeUnit, State
except:
    print "Please install BigJob!"


###################################################################################################
if __name__ == '__main__':

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


    logger.debug("Create Data Unit for Publishing (Producer)")
    f = open("examples/data/data_20points.csv")
    points = f.readlines()
    f.close()
    du_points = DistributedInMemoryDataUnit("Points", pilot=pilot)
    du_points.load(points)





    end_data_load = time.time()
    time_measures["DataLoadTime"] = end_data_load-end_start_pilot

    for iteration in range(0,NUM_ITERATIONS):
        iteration_start = time.time()
        future = du_points.map_pilot("KMeans.closestPoint", du_centers.name, number_of_compute_units=2)
        output_dus = future.result()
        new_centers = []
        for du in output_dus:
            future=du.reduce_pilot("KMeans.averagePoints")
            result_du = future.result()
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



