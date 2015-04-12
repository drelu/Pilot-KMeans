'''
Spark In-Memory DataUnit

@author: Andre Luckow
'''
from __builtin__ import classmethod
import argparse
import os, sys, time
import logging
import types
import threading
import importlib
import itertools
import uuid
import inspect

logger = logging.getLogger('DistributedInMemoryDataUnitSpark')
logger.setLevel(logging.DEBUG)


#######################
# Mac OS:
# brew install apache-spark
# SPARK_HOME='/usr/local/Cellar/apache-spark/1.1.0/libexec/'
# Start Spark: /usr/local/Cellar/apache-spark/1.1.0/libexec/sbin/start-all.sh
#
# py4j needs be installed in your virtualenv

SPARK_HOME='/usr/local/Cellar/apache-spark/1.1.0/libexec/'
os.environ["SPARK_HOME"]=SPARK_HOME

import sys
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
from pyspark import SparkContext, SparkConf


# Global variables
rddDict={}
spark_context=None


class Future(object):
    
    
    def __init__(self, output):
        #self.distributed_inmemory_dataunit=distributed_inmemory_dataunit
        self.output=output

    def cancel(self):
        """Attempt to cancel the call. If the call is currently being executed and cannot be 
           cancelled then the method will return False, otherwise the call will be 
           cancelled and the method will return True."""
        pass

    def get_state(self):
        """ get state of current pilot-based processing, i.e. the map resp. reduce phase"""
        pass
    
    
    def result(self): 
        """Return the value returned by the call. If the call hasn't yet completed then this
           method will wait for the result """
        return self.output


class SparkTaskWrapper(object):


    def __init__(self, function=None, args=None):
        self.args = args

        if isinstance(function, str):
            self.function = function
            # dynamically load function reference using function name (string)
            # parse out class and function name
            classname = self.function.split(".")[0]
            functionname = self.function.split(".")[1]
            # get reference to calling module
            frm = inspect.stack()
            frm = frm[3]
            mod = inspect.getmodule(frm[0])
            #logger.debug("Calling module: " + str(mod))
            class_pointer = getattr(mod, classname)
            self.function_pointer = getattr(class_pointer, functionname)
        else:
            self.function_pointer=function

        print str(self.function_pointer)


    def execute(self, rdd_data):
        #import kmeans.kmeans_spark
        #kmeans.kmeans_spark.KMeans.closestPoint(rdd_data, self.args)
        return self.function_pointer(rdd_data, self.args)



class DistributedInMemoryDataUnit(object):
    """ In-Memory DU backed by a Spark RDD """


    def __init__(self, name="test-dimdu", url="spark://localhost:7077",
                 pilot=None, sc=None):
        global spark_context
        if sc==None and spark_context==None:
            #conf = SparkConf().setAppName("Pilot-Data-InMemory").setMaster(url)
            #spark_context = SparkContext(conf)
            spark_context = SparkContext(url, "Pilot-InMemory", sparkHome=os.environ["SPARK_HOME"])
        else:
            spark_context=sc

        self.sc = spark_context

        self.parallelism=1
        if pilot!=None and pilot.has_key("number_of_processes"):
            self.parallelism=int(pilot["number_of_processes"])

        self.sc=spark_context
        self.url = url
        self.resource_lock = threading.RLock()
        self.name=name
        self.data = None


    def load(self, data=[]):
        self.data = self.sc.parallelize(data, self.parallelism)
        rddDict[self.name]=self.data
    
    def reload(self, data=[]):
        self.load(data)


    def delete(self):
        pass


    def map_pilot(self, function, args, 
                  partition_prefix="map-part",  
                  number_of_compute_units=1,
                  number_of_cores_per_compute_unit=1):        
        """ Execute map function using a set of CUs on the Pilot 
            TODO: add partitioning
        """
        return self.map(function, args, number_of_compute_units)
        

    def reduce_pilot(self, function,
                     partition_prefix="reduce-part",
                     number_of_compute_units=1
                     ):  
        return self.reduce(function, number_of_compute_units=number_of_compute_units)


    def map(self, function, args, start=0, end=None, number_of_compute_units=None):
        data = None
        if rddDict.has_key(args):
            rdd = rddDict[args]
            data = rdd.collect()

        spark_map = SparkTaskWrapper(function, data)
        result_rdd = self.data.map(spark_map.execute)
        result_data = result_rdd.collect()
        #print("Results of Map: " + str(result_data))
        output_du = DistributedInMemoryDataUnit("output-du", sc=self.sc,
                                                pilot={"number_of_processes": self.parallelism})
        output_du.data=result_rdd
        return Future([output_du])


    def reduce(self, function, args=None, number_of_compute_units=1):

        if isinstance(function, str):
            classname = function.split(".")[0]
            functionname = function.split(".")[1]
            # get reference to calling module
            frm = inspect.stack()
            frm = frm[2]
            mod = inspect.getmodule(frm[0])
            #logger.debug("Calling module: " + str(mod))
            class_pointer = getattr(mod, classname)
            function_pointer = getattr(class_pointer, functionname)
            print str(function_pointer)
        else:
            function_pointer=function

        print self.data.collect()
        print "****************************************************"

        grouped_data = self.data.groupByKey(number_of_compute_units)
        #print grouped_data.collect()
        print "****************************************************"

        #group_data_string = grouped_data.map(lambda a: "(%s,%s)"%(a[0],str([i for i in a[1]])))

        group_data_string = grouped_data.map(lambda a: "(%s,%s)"%(a[0], str(sorted(set(a[1])))))

        #group_data_string = grouped_data.map(lambda a: "(%d,%s)"%(a[0],[i for i in a[1]]))
        #group_data_string = grouped_data.map(lambda a: "%s"%([str((a[0],i)) for i in a[1]]), number_of_compute_units)
        #print group_data_string.collect()
        print "********* Apply Reduce Function"
        result_rdd=group_data_string.flatMap(function_pointer, number_of_compute_units)
        #print result_rdd.collect()
        output_du = DistributedInMemoryDataUnit("output-du", sc=self.sc,
                                                pilot={"number_of_processes": self.parallelism})
        output_du.data=result_rdd
        return Future(output_du)


    def merge(self, du_list=[]):
        """ Merge Data Units contents with this DU """
        if self.data == None:
            self.data=du_list[0].data

        del du_list[0]
        for i in du_list:
            self.data = self.data.join(i)

        rddDict[self.name]=self.data
        return self


    def export(self):
        return self.data.collect()
     



if __name__ == '__main__':
    """
    Test for Spark-based DU
    
    """

    du = DistributedInMemoryDataUnit(name="Helloworld", url="spark://localhost:7077")

    f = open("centers.csv")
    centers = f.readlines()
    f.close()
    du.load(centers)
