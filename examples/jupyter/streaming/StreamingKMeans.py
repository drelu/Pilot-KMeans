#!/bin/python
#
# /home/01131/tg804093/work/spark-2.0.0-bin-hadoop2.6/bin/spark-submit --master spark://c251-121.wrangler.tacc.utexas.edu:7077 --packages  org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 StreamingApp.py

import os
import sys
import time
start = time.time()
import logging
logging.basicConfig(level=logging.WARN)
sys.path.append("../util")
import init_spark_wrangler
from pilot_hadoop import PilotComputeService as PilotSparkComputeService
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import numpy as np

SPARK_MASTER="spark://c251-141.wrangler.tacc.utexas.edu:7077"
SPARK_LOCAL_IP="129.114.58.2"
KAFKA_ZK='c251-121.wrangler.tacc.utexas.edu:2181'

os.environ["SPARK_LOCAL_IP"]=SPARK_LOCAL_IP

pilotcompute_description = {
    "service_url": SPARK_MASTER,
    "spark.driver.host": os.environ["SPARK_LOCAL_IP"]
}


print "SPARK HOME: %s"%os.environ["SPARK_HOME"]
print "PYTHONPATH: %s"%os.environ["PYTHONPATH"]

start = time.time()
pilot_spark = PilotSparkComputeService.create_pilot(pilotcompute_description=pilotcompute_description)
sc = pilot_spark.get_spark_context()
#print str(sc.parallelize([2,3]).collect())
print "Spark Startup, %.2f"%(time.time()-start)

#######################################################################################
ssc = StreamingContext(sc, 10)
kafka_dstream = KafkaUtils.createStream(ssc, KAFKA_ZK, "spark-streaming-consumer", {'kmeans': 1})  
# We create a model with random clusters and specify the number of clusters to find
model = StreamingKMeans(k=2, decayFactor=1.0).setRandomCenters(3, 1.0, 0)
model.trainOn(kafka_dstream.map(lambda point: np.array(point)))



# Now register the streams for training and testing and start the job,
# printing the predicted cluster assignments on new data points as they arrive.
#model.trainOn(trainingStream)
#result = model.predictOnValues(testingStream.map(lambda point: ))
#result.pprint()

#lines = kvs.map(lambda x: x[1])
#counts = lines.flatMap(lambda line: line.split(" ")) \
#        .map(lambda word: (word, 1)) \
#        .reduceByKey(lambda a, b: a+b)
#counts.pprint()


ssc.start()
ssc.awaitTermination()
ssc.stop(stopSparkContext=True, stopGraceFully=True)