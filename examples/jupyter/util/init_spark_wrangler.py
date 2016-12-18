import os, sys, time

SPARK_HOME="/home/01131/tg804093/work/spark-2.0.2-bin-hadoop2.6" 

if not os.environ.has_key("SPARK_HOME"):
    os.environ["SPARK_HOME"]=SPARK_HOME
else:
    SPARK_HOME=os.environ["SPARK_HOME"]
    

#print "Init Spark: " + SPARK_HOME

os.environ["PYSPARK_PYTHON"]="/home/01131/tg804093/anaconda2/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"]="ipython"
os.environ["PYSPARK_DRIVER_PYTHON_OPTS"]="notebook"
os.environ["PYTHONPATH"]= os.path.join(SPARK_HOME, "python")+":" + os.path.join(SPARK_HOME, "python/lib/py4j-0.10.1-src.zip")
    
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
#sys.path.insert(0, os.path.join(SPARK_HOME, 'python/lib/py4j-0.9-src.zip')) 
sys.path.insert(0, os.path.join(SPARK_HOME, 'python/lib/py4j-0.10.1-src.zip')) 
sys.path.insert(0, os.path.join(SPARK_HOME, 'bin') )

# import Spark Libraries
from pyspark import SparkContext, SparkConf, Accumulator, AccumulatorParam
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.mllib.linalg import Vector

import pandas as pd
from IPython.display import HTML

YARN_HOST="http://yarn-aws.radical-cybertools.org:8088"

def print_application_url(yarn_output):
    applications=[]
    for line in yarn_output:
        comp = line.split('\t')
        if comp[0].startswith("application"):
            user=comp[3].strip()
            url=YARN_HOST+"/proxy/" + comp[0].strip()
            application_url="<a target=blank href='"+url+"'>"+url+"</a>"
            applications.append((user, comp[1], application_url))

    pd.set_option('max_colwidth', 1000)
    df=pd.DataFrame(applications, columns=["User", "Name", "Spark Application URL"])
    return HTML(df.to_html(escape=False))


#sc = SparkContext("spark://c251-119.wrangler.tacc.utexas.edu:7077")
#print "Initialized SC"
#sc.parallelize([1,2,3]).collect()
