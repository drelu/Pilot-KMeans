import os, sys, time
import logging
logger = logging.getLogger('DistributedInMemoryDataUnit-KMeans')
logger.setLevel(logging.DEBUG)
import redis
import threading
import numpy as np
import itertools
import datetime
import traceback

try:
    from pilot import PilotComputeService, PilotCompute, ComputeUnit, State
    from distributed_inmem.dataunit_kafka import DistributedInMemoryDataUnit
except  Exception as e:
    print "Please install BigJob!"




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


class MyClass(object):

    @staticmethod
    def add_points(data):
        return 1


##################################################################################
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
    f = open("data/data_20points.csv")
    points = f.readlines()
    f.close()
    du_points = DistributedInMemoryDataUnit("Points", pilot=pilot)
    du_points.load(points)
    end_data_load = time.time()
    time_measures["DataLoadTime"] = end_data_load-end_start_pilot

    iteration_start = time.time()
    future = du_points.map(module_name=__name__, function_name="MyClass.add_points")
    output_dus = future.result()

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



