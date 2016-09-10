# Pilot-Data In-Memory for Iterative Computations and Pilot-Streaming

# SAGA-Hadoop

[SAGA-Hadoop] can be used to spawn Spark and Kafka clusters: <https://github.com/drelu/SAGA-Hadoop>. SAGA-Hadoop enables you to setup an environment usable for building data-intensive streaming applications on HPC.


# Pilot-Memory (KMeans)

## Spark

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

        kafka-topics --create --zookeeper localhost:2181 --topic test

        kafka-topics --delete --zookeeper localhost:2181 --topic test


## Kafka and Spark on HPC

For usage on a HPC machine, see example <https://github.com/drelu/Pilot-Memory/blob/master/examples/jupyter/streaming/Pilot-Spark-Wrangler.ipynb>
    
        login1.wrangler ~/work$ saga-hadoop --resource=slurm://localhost              --queue=normal --walltime=59 --number_cores=24              --project=TG-MCB090174 --framework spark
         
        SPARK installation directory: /home/01131/xxx/work/work/spark-2.0.0-bin-hadoop2.6
        (please allow some time until the SPARK cluster is completely initialized)
        export PATH=/home/01131/xxx/work/work/spark-2.0.0-bin-hadoop2.6/bin:$PATH
        Spark Web URL: http://c251-135:8080
        Spark Submit endpoint: spark://c251-135:7077
    
