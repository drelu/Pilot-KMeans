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
    import saga
    from pilot import PilotComputeService, PilotCompute, ComputeUnit, PilotDataService, PilotData, DataUnit, State
    from distributed_inmem.dataunit_kafka import DistributedInMemoryDataUnit
except  Exception as e:
    print "Please install BigJob!"

COORDINATION_URL = "redis://localhost:6379"


def start_pilotcompute(pilot_compute_description=None):
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)
    if pilot_compute_description==None:
        pilot_compute_description = {
            "service_url": 'fork://localhost',
            "number_of_processes": 2,
            "working_directory": os.getcwd() + "/work/",
        }
    pilotcompute = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    return pilotcompute

def create_pilotdata():
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    pilot_data_description={
        "service_url": "ssh://localhost/tmp/pilot-data/",
    }
    pilotdata=pilot_data_service.create_pilot(pilot_data_description=pilot_data_description)
    return pilotdata


##################################################################################
class StreamingKMeans(object):

    @staticmethod
    def cluster_points(data):
        return 1


##################################################################################
if __name__ == '__main__':

    run_timestamp=datetime.datetime.now()
    time_measures={}
    print __name__

    ##############################################################################
    start = time.time()
    pilotcompute=start_pilotcompute()
    end_start_pilot = time.time()
    time_measures["Pilot Submission"]=end_start_pilot-start
    logger.debug("Started pilot in %.2f sec"%time_measures["Pilot Submission"])

    # Not used for now... data is written back to streaming du in kafka
    #pilotdata = create_pilotdata()
    #data_unit = pilotdata.submit_data_unit({})

    ##############################################################################


    logger.debug("Create Data Unit for Publishing (Producer)")
    f = open("data/data_20points.csv")
    points = f.readlines()
    f.close()
    du_points = DistributedInMemoryDataUnit("Points", pilot=pilotcompute)
    du_points.load(points)
    end_data_load = time.time()
    time_measures["DataLoadTime"] = end_data_load-end_start_pilot

    iteration_start = time.time()
    ## returns list of compute units
    compute_units = du_points.map_pilot(module_name=__name__,
                                        function_name="StreamingKMeans.cluster_points",
                                        args=None,
                                        number_of_compute_units=None,
                                        number_of_cores_per_compute_unit=1,
                                        output_du_name="kafka-output")

    end = time.time()
    time_measures["Runtime"] = end-start

    output_du = DistributedInMemoryDataUnit(name="kafka-output")
    output_du.export("/tmp/kafka-output")

    ##############################################################################
    # print output
    # try:
    #     os.mkdir(RESULT_DIR)
    # except:
    #     pass
    #
    # output_data = open(os.path.join(RESULT_DIR, PERFORMANCE_DATA_FILE + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"), "w")
    # header=",".join(["KMeansImpl", "NumberPoints", "NumberCentroids", "NumberIterations" "Pilot", "NumberCores" "RunTimestamp", "Type", "Value"])
    # line = ("DIDU-KMeans", str(number_of_data_points), str(number_of_centroids_points),
    #         str(NUM_ITERATIONS), pilot_compute_description["service_url"],
    #         str(pilot_compute_description["number_of_processes"]), run_timestamp.isoformat())
    #
    # for time_type, value in time_measures.items():
    #     print_string=",".join(line + (time_type, str(value)))
    #     output_data.write(print_string + "\n")
    #
    # output_data.close()



