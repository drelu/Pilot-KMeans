#!/bin/python
#
# /home/01131/tg804093/work/spark-2.0.0-bin-hadoop2.6/bin/spark-submit --master spark://c251-121.wrangler.tacc.utexas.edu:7077 --packages  org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 StreamingApp.py

import os
import sys
import time
import logging
logging.basicConfig(level=logging.WARN)

sys.path.append("../util")
#os.environ["PYSPARK_SUBMIT_ARGS"]="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0"

import init_spark_wrangler
from pilot_hadoop import PilotComputeService as PilotSparkComputeService

#os.environ["PYSPARK_SUBMIT_ARGS"]="--jars /home/01131/tg804093/src/spark-streaming-kafka/spark-streaming-kafka-0-10-assembly_2.11-2.0.0.jar"
#os.environ["SPARK_CLASSPATH"]=":".join(["/home/01131/tg804093/" + i for i in files])

#os.environ["SPARK_CLASSPATH"]="/home/01131/tg804093/spark-streaming-kafka-0-8_2.11-2.0.0.jar"

os.environ["SPARK_LOCAL_IP"]="129.114.58.2"

pilotcompute_description = {
    "service_url": "spark://c251-121.wrangler.tacc.utexas.edu:7077",
    #"spark.jars.packages": "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0",
    #"spark.jars": "/home/01131/tg804093/spark-streaming-kafka-0-8_2.11-2.0.0.jar"
    "spark.driver.host": os.environ["SPARK_LOCAL_IP"]
}

print "SPARK HOME: %s"%os.environ["SPARK_HOME"]
print "PYTHONPATH: %s"%os.environ["PYTHONPATH"]

start = time.time()
pilot_spark = PilotSparkComputeService.create_pilot(pilotcompute_description=pilotcompute_description)
sc = pilot_spark.get_spark_context()
print str(sc.parallelize([2,3]).collect())
print "Spark Startup, %.2f"%(time.time()-start)


zkKafka='c251-118.wrangler.tacc.utexas.edu:2181'
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
ssc = StreamingContext(sc, 30)
kvs = KafkaUtils.createStream(ssc, zkKafka, "spark-streaming-consumer", {'test': 1})  
lines = kvs.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
counts.pprint()
ssc.start()
ssc.awaitTermination()