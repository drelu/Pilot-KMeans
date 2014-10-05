# Pilot-Data In-Memory for Iterative Computations


# Pilot-KMeans

For Spark all Python dependencies must be pre-installed on all nodes!


 1. Build Egg:
 
        python setup.py bdist_egg


 2. Run on Spark       
  
        spark-submit --master spark://192.168.0.3 --py-files src/kmeans/kmeans_spark.py, src/distributed_inmem/dataunit_spark.py src/kmeans/kmeans_spark.py