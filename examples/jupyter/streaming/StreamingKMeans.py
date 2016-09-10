#!/bin/python
#
# /home/01131/tg804093/work/spark-2.0.0-bin-hadoop2.6/bin/spark-submit --master spark://c251-121.wrangler.tacc.utexas.edu:7077 --packages  org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 StreamingApp.py

import os
import sys
import pickle
import time
import datetime
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
import msgpack
import msgpack_numpy as m
m.patch()

run_timestamp=datetime.datetime.now()

SPARK_MASTER="spark:///c251-135.wrangler.tacc.utexas.edu:7077"
SPARK_LOCAL_IP="129.114.58.134"
KAFKA_ZK='c251-137.wrangler.tacc.utexas.edu:2181'
METABROKER_LIST='c251-137.wrangler.tacc.utexas.edu:9092'
TOPIC='kmeans_list'
RESULT_FILE= "results/results-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"

try:
    os.makedirs("results")
except:
    pass

output_file=open(RESULT_FILE, "w")

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
output_file.write("Spark Startup, %d, %.2f\n"%(-1, time.time()-start))

#######################################################################################

decayFactor=1.0
timeUnit="batches"
model = StreamingKMeans(k=10, decayFactor=decayFactor, timeUnit=timeUnit).setRandomCenters(3, 1.0, 0)

def printOffsetRanges(rdd):
    for o in offsetRanges:
        print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

def count_records(rdd):    
    print str(type(rdd))
    if rdd!=None:
        return rdd.collect()
    
    return [0]
        
def pre_process(datetime, rdd):  
    #print (str(type(time)) + " " + str(type(rdd)))    
    start = time.time()    
    points=rdd.map(lambda p: p[1]).flatMap(lambda a: eval(a)).map(lambda a: Vectors.dense(a))
    end_preproc=time.time()
    count = points.count()
    output_file.write("KMeans PreProcess, %d, %.5f\n"%(count, end_preproc-start))
    output_file.flush()
    return points
    #points.pprint()
    #model.trainOn(points)

def model_update(rdd):
    count = rdd.count()
    start = time.time()
    lastest_model = model.latestModel()
    lastest_model.update(rdd, decayFactor, timeUnit)    
    end_train = time.time()
    #predictions=model.predictOn(points)
    #end_pred = time.time()    
    output_file.write("KMeans Model Update, %d, %.3f\n"%(count, end_train-start))
    output_file.flush()
    #output_file.write("KMeans Prediction, %.3f\n"%(end_pred-end_train))
    #return predictions
    

def model_prediction(rdd):
    pass

    
ssc_start = time.time()    
ssc = StreamingContext(sc, 10)
#kafka_dstream = KafkaUtils.createStream(ssc, KAFKA_ZK, "spark-streaming-consumer", {TOPIC: 1})
#kafka_param: "metadata.broker.list": brokers
kafka_dstream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": METABROKER_LIST })
ssc_end = time.time()    
output_file.write("Spark SSC Startup, %d, %.2f\n"%(-1, ssc_end-ssc_start))


#counts=[]
#kafka_dstream.foreachRDD(lambda t, rdd: counts.append(rdd.count()))
#global count_messages 
#count_messages  = sum(counts)
#
#output_file.write(str(counts))
kafka_dstream.count().pprint()

#print str(counts)
#count = kafka_dstream.count().reduce(lambda a, b: a+b).foreachRDD(lambda a: a.count())
#if count==None:
#    count=0
#print "Number of Records: %d"%count


points = kafka_dstream.transform(pre_process)
points.pprint()
points.foreachRDD(model_update)

#predictions=model_update(points)
#predictions.pprint()



#We create a model with random clusters and specify the number of clusters to find
#model = StreamingKMeans(k=10, decayFactor=1.0).setRandomCenters(3, 1.0, 0)
#points=kafka_dstream.map(lambda p: p[1]).flatMap(lambda a: eval(a)).map(lambda a: Vectors.dense(a))
#points.pprint()


# Now register the streams for training and testing and start the job,
# printing the predicted cluster assignments on new data points as they arrive.
#model.trainOn(trainingStream)
#result = model.predictOnValues(testingStream.map(lambda point: ))
#result.pprint()

# Word Count
#lines = kvs.map(lambda x: x[1])
#counts = lines.flatMap(lambda line: line.split(" ")) \
#        .map(lambda word: (word, 1)) \
#        .reduceByKey(lambda a, b: a+b)
#counts.pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop(stopSparkContext=True, stopGraceFully=True)

output_file.close()
