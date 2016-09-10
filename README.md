# Pilot-Data In-Memory for Iterative Computations

# SAGA-Hadoop

[SAGA-Hadoop] can be used to spawn Spark and Kafka clusters: <https://github.com/drelu/SAGA-Hadoop>


## Spark




# Pilot-Memory (KMeans)

For Spark all Python dependencies must be pre-installed on all nodes!


 1. Build Egg:
 
        python setup.py bdist_egg


 2. Run on Spark       
  
        spark-submit --master spark://192.168.0.3 --py-files src/kmeans/kmeans_spark.py, src/distributed_inmem/dataunit_spark.py src/kmeans/kmeans_spark.py


## Kafka

1. Start a local Kafka:

    saga-hadoop --framework kafka
    

    
2. Check Kafka Cluster
    
    kafka-topics --list --zookeeper localhost:2181

## Kafka and Spark on HPC

For usage on a HPC machine, see example <https://github.com/drelu/Pilot-Memory/blob/master/examples/jupyter/streaming/Pilot-Spark-Wrangler.ipynb>
    
    
    
    kafka-topics --delete --zookeeper localhost:2181 --topic test
