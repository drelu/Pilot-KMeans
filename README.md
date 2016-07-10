# Pilot-Data In-Memory for Iterative Computations

# SAGA-Hadoop

[SAGA-Hadoop] can be used to spawn Spark and Kafka clusters: <https://github.com/drelu/SAGA-Hadoop>


## Spark

kafka-topics --list --zookeeper localhost:2181
Points
pykafka-test-topic2
test2


## Kafka

Start a local Kafka:

    saga-hadoop --framework kafka


# Pilot-Memory (KMeans)

For Spark all Python dependencies must be pre-installed on all nodes!


 1. Build Egg:
 
        python setup.py bdist_egg


 2. Run on Spark       
  
        spark-submit --master spark://192.168.0.3 --py-files src/kmeans/kmeans_spark.py, src/distributed_inmem/dataunit_spark.py src/kmeans/kmeans_spark.py



### Other Notes

Delete Kafka topic:

    kafka-topics.sh --delete --zookeeper localhost:2181 --topic test
