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


Spark
    
        login1.wrangler ~/work$ saga-hadoop --resource=slurm://localhost --queue=normal \ 
                                            --walltime=59 --number_cores=24 \
                                            --project=xxxx --framework spark
         
        SPARK installation directory: /home/01131/xxx/work/work/spark-2.0.0-bin-hadoop2.6
        (please allow some time until the SPARK cluster is completely initialized)
        export PATH=/home/01131/xxx/work/work/spark-2.0.0-bin-hadoop2.6/bin:$PATH
        Spark Web URL: http://c251-135:8080
        Spark Submit endpoint: spark://c251-135:7077
    

Kafka

         login1.wrangler ~$ saga-hadoop --resource=slurm://localhost --queue=normal \
                                        --walltime=59 --number_cores=24 \
                                        --project=xx --framework kafka
         Kafka Config: /home/01131/tg804093/work/kafka-2434be50-7770-11e6-9675-b083fed043f0/config (Sat Sep 10 11:03:32 2016)
         broker.id: 0
         listeners: PLAINTEXT://c251-137:9092
         zookeeper.connect: c251-137:2181
         zookeeper.connection.timeout.ms: 6000
