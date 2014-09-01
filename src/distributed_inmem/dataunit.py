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
import uuid
logger = logging.getLogger('DistributedInMemoryDataUnit')
logger.setLevel(logging.WARNING)
#from pilot import PilotComputeService, PilotCompute, ComputeUnit, State
 
   
# 


class DistributedInMemoryDataUnit():  
    """ In-Memory DU backed by Redis """
            
    def __init__(self, name="test-dimdu",
                 hostname="localhost" ,
                 port=6379,
                 flushdb=False, 
                 pilot=None):
        self.redis_connection_pool = redis.ConnectionPool(host=hostname, port=6379, password=None, db=0)
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
        
        
    def map_pilot(self, function, args, 
                  partition_prefix="map-part",
                  number_of_processes=1):        
        """ Execute map function using a set of CUs on the Pilot 
            TODO: add partitioning
        """
        prefix = partition_prefix + "-" + str(uuid.uuid4())[:8]
        # start compute unit
        compute_unit_description = {
            "executable": "python",
            "arguments": ["-m", "distributed_inmem.dataunit", 
                          "-n", self.name,  
                          "-m", "kmeans.kmeans", 
                          "--map_function", function, 
                          "--args",  args, 
                          "--output_du_prefix", prefix],
            "number_of_processes": number_of_processes,
            "output": "stdout.txt",
            "error": "stderr.txt", 
        }
        compute_unit = self.pilot.submit_compute_unit(compute_unit_description)
        compute_unit.wait()
        dus = self.__get_output_du(prefix)
        return dus
    
        
    def reduce_pilot(self, function, 
                     partition_prefix="reduce-part",
                     number_of_processes=1
                     ):  
        """ Execute reduce function using a set of CUs on the Pilot 
            TODO: add partitioning
        """
         
        prefix = partition_prefix + "-" + str(uuid.uuid4())[:8]
        # start compute unit
        compute_unit_description = {
            "executable": "python",
            "arguments": ["-m", "distributed_inmem.dataunit", 
                          "-n", self.name,  
                          "-m", "kmeans.kmeans", 
                          "--reduce_function", function,
                          "--output_du_prefix", prefix],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt", 
        }
        compute_unit = self.pilot.submit_compute_unit(compute_unit_description)
        compute_unit.wait()
        dus = self.__get_output_du(prefix)
        return dus[0]


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
        result = None
        if args==None:
            result = function(points)
        else: 
            # check weather arg is an DU that needs to get loaded
            if type(args)==types.StringType and self.redis_client.exists(args):
                args = DistributedInMemoryDataUnit(name=args).export()
            if args.__class__.__name__==DistributedInMemoryDataUnit.__name__:
                args = args.export()
            result = function(points, args)
        return result
    

    def merge(self, du_list=[]):
        """ Merge Data Units contents with this DU """
        
        data = []
        # get data of DU
        self.pipe.watch(self.name)
        end = int(self.pipe.llen(self.name)) 
        if end > 0: data.append(self.pipe.lrange(self.name, 0, end))
        
        # get data of other dus
        for du in du_list:
            self.pipe.watch(du.name)
            end = self.pipe.llen(du.name)
            data.append(self.pipe.lrange(du.name, 0, end))
        
        # Upload new data to this DU
        #results = self.pipe.execute()
        self.pipe.multi()
        self.len=0
        for line in data:
            self.pipe.rpush(self.name, line[0])
            self.len = self.len + 1
        
        results = self.pipe.execute()
        logger.debug("New data after merge: " + str(data))
        return self
    
        
    
    def export(self):
        end = self.redis_client.llen(self.name)
        return self.redis_client.lrange(self.name, 0, end)
     
    
    @staticmethod
    def flushdb():
        redis_client = redis.Redis(host="localhost", port=6379, password=None, db=0)
        redis_client.flushdb()

    
    ###########################################################################
    def __get_output_du(self, prefix):
        names=self.redis_client.keys(prefix+"*")
        dus = []
        for n in names:
            du = DistributedInMemoryDataUnit(n, pilot=self.pilot)
            dus.append(du)
        return dus
    
   



if __name__ == '__main__':
    """ Initiate a DataUnit an run a map or reduce function on it 
    
       python -m distributed_inmem.dataunit -n Points -m kmeans.kmeans --map_function KMeans.closestPoint --args Centers --shuffle_du_prefix=part
    
    """
    print "Start worker task for distributed in-memory dataunit"
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
    parser.add_argument('--output_du_prefix')        
    
    
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
    
    name = parsed_arguments.name
    du = DistributedInMemoryDataUnit(name)
    module = parsed_arguments.module
    args = None
    if parsed_arguments.args!=None:
        args = parsed_arguments.args
    
    print "Load module: " + module
    mod = importlib.import_module(module)
    print(str(dir(mod)))
    
    map_function = parsed_arguments.map_function
    reduce_function = parsed_arguments.reduce_function
        
    map_reduce_result = []
    if map_function!=None:
        classname = map_function.split(".")[0]
        functionname = map_function.split(".")[1]
        class_pointer = getattr(mod, classname)
        function_pointer = getattr(class_pointer, functionname)
        map_reduce_result = du.map(function_pointer, args)
        print(str(map_reduce_result))
        if parsed_arguments.output_du_prefix!=None:
            prefix = parsed_arguments.output_du_prefix
            map_reduce_result.sort(key=lambda tup: tup[0])
            print "Map Result: " + str(map_reduce_result)
            dus = {}
            for key, group in itertools.groupby(map_reduce_result, lambda x: x[0]):
                partition = prefix+":"+str(key)
                if not dus.has_key(partition):
                    dus[partition] = DistributedInMemoryDataUnit(name=partition)
                dus[partition].load(group)    
    elif reduce_function!=None:
        classname = reduce_function.split(".")[0]
        functionname = reduce_function.split(".")[1]
        class_pointer = getattr(mod, classname)
        function_pointer = getattr(class_pointer, functionname)
        map_reduce_result = du.reduce(function_pointer, args)
        print("Reduce Result: " + str(map_reduce_result))
        if parsed_arguments.output_du_prefix!=None:
            prefix = parsed_arguments.output_du_prefix
            du_name = prefix + "-" + name
            print "Export result to DU:" + prefix + "-" + du_name
            du = DistributedInMemoryDataUnit(name=du_name)
            du.load([map_reduce_result])    
    
   

    