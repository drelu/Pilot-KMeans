"""
Produces batches of points for stream processing
"""

import sys
import numpy as np
import time
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
NUMBER_OF_POINTS=10
NUMBER_OF_DIMENSION=3

if __name__ == '__main__':

    time_measures ={}
    points=np.random.normal(size=(10,3))
    du_points = DistributedInMemoryDataUnit("Points")
    du_points.load(points)
    end_data_load = time.time()
    time_measures["DataLoadTime"] = end_data_load-end_start_pilot


