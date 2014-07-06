'''
Created on Jul 5, 2014

@author: Andre Luckow
'''
import redis
import argparse
import os, sys, time
import logging
import types
import threading
import importlib
import itertools
logger = logging.getLogger('DistributedInMemoryDataUnit')

#from pilot import PilotComputeService, PilotCompute, ComputeUnit, State
 
   

class DistributedInMemoryDataUnit():  
    """ In-Memory DU backed by Redis """
            
    def __init__(self, name="test-dimdu", flushdb=False, pilot=None):
        self.redis_connection_pool = redis.ConnectionPool(host="localhost", port=6379, password=None, db=0)
        self.redis_client = redis.Redis(connection_pool=self.redis_connection_pool)
        #self.redis_client_pubsub = self.redis_client.pubsub() # redis pubsub client       
        self.resource_lock = threading.RLock()
        self.pipe = self.redis_client.pipeline()
        self.name=name
        try:
            self.redis_client.ping()
        except Exception, ex:
            logger.error("Cannot connect to Redis server: %s" % str(ex))
            raise Exception("Cannot connect to Redis server: %s" % str(ex))
        self.data = []
        self.len = 0
        self.pilot=pilot
        if flushdb:
            self.redis_client.flushdb()
        
        
    def load(self, data=[]):
        for i in data:
            self.pipe.rpush(self.name, i)
            self.len = self.len + 1
        self.pipe.execute()
    
    
    def reload(self, data=[]):
        self.pipe.delete(self.name)
        self.len=0
        self.load(data)        
        
        
    def map_pilot(self, function, args, partition_prefix="part"):
        # start compute unit
        compute_unit_description = {
            "executable": "python",
            "arguments": ["-m", "inmem.dataunit", 
                          "-n", self.name,  
                          "-m", "kmeans.kmeans", 
                          "--map_function", "KMeans.closestPoint", 
                          "--args",  args.name, 
                          "--shuffle_du_prefix", partition_prefix],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt", 
        }
        compute_unit = self.pilot.submit_compute_unit(compute_unit_description)
        compute_unit.wait()
        

    def map(self, function, args, start=0, end=None):
        if end==None:
            end = self.redis_client.llen(self.name)
        points = self.redis_client.lrange(self.name, start, end)
        result = []
        for p in points:
            if args==None:
                result.append(function(p))
            else: 
                # check weather arg is an DU that needs to get loaded
                if type(args)==types.StringType and self.redis_client.exists(args):
                    args = DistributedInMemoryDataUnit(name=args).export()
                if args.__class__.__name__==DistributedInMemoryDataUnit.__name__:
                    args = args.export()
                result.append(function(p, args))
        return result


    def reduce(self, function, args):
        end = self.redis_client.llen(self.name)
        points = self.redis_client.lrange(self.name, 0, end)
        return function(points)
    
    
    def export(self):
        end = self.redis_client.llen(self.name)
        return self.redis_client.lrange(self.name, 0, end)
    
    
    @staticmethod
    def flushdb():
        redis_client = redis.Redis(host="localhost", port=6379, password=None, db=0)
        redis_client.flushdb()

    
   



if __name__ == '__main__':
    """ Initiate a DataUnit an run a map or reduce function on it 
    
       python -m inmem.dataunit -n Points -m kmeans.kmeans --map_function KMeans.closestPoint --args Centers --shuffle_du_prefix=part
    
    """
    
    distributed_data_unit = DistributedInMemoryDataUnit()
    parser = argparse.ArgumentParser(add_help=True, description="""DistributedInMemoryDataUnit Startup Utility""")
    
    parser.add_argument('--coordination', '-c', default="redis://localhost")
    parser.add_argument('--password', '-p', default="")
    parser.add_argument('--name', '-n')
    parser.add_argument('--partition_start', '-ps')
    parser.add_argument('--partition_end', '-pe')      
    parser.add_argument('--module', '-m')
    parser.add_argument('--map_function', '-mf')
    parser.add_argument('--reduce_function', '-rf')    
    parser.add_argument('--args', '-a')        
    parser.add_argument('--shuffle_du_prefix')        
    
    
    parsed_arguments = parser.parse_args()  
    if parsed_arguments.name==None:
        print "Error! Please specify name of Data Unit"
        sys.exit(-1)
    elif parsed_arguments.map_function==None and parsed_arguments.reduce_function==None:
        print "Error! Please specify map or reduce function"
        sys.exit(-1)
    elif parsed_arguments.module==None:
        print "Error! Please specify module"
        sys.exit(-1)
    
    du = DistributedInMemoryDataUnit(parsed_arguments.name)
    module = parsed_arguments.module
    args = parsed_arguments.args
    
    print "Load module: " + module
    mod = importlib.import_module(module)
    print(str(dir(mod)))
    
    map_function = parsed_arguments.map_function
    reduce_function = parsed_arguments.reduce_function
        
    best_centers = []
    if map_function!=None:
        classname = map_function.split(".")[0]
        functionname = map_function.split(".")[1]
        class_pointer = getattr(mod, classname)
        function_pointer = getattr(class_pointer, functionname)
        best_centers = du.map(function_pointer, args)
        print(str(best_centers))
    
    if parsed_arguments.shuffle_du_prefix!=None:
        prefix = parsed_arguments.shuffle_du_prefix
        best_centers.sort(key=lambda tup: tup[0])
        dus = {}
        for key, group in itertools.groupby(best_centers, lambda x: x[0]):
            partition = prefix+"-"+str(key)
            if not dus.has_key(partition):
                dus[partition] = DistributedInMemoryDataUnit(name=partition)
            dus[partition].load(group)    

    