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
logger.setLevel(logging.DEBUG)
#from pilot import PilotComputeService, PilotCompute, ComputeUnit, State
 
   
class Future(object):
    
    
    def __init__(self, pilot, compute_units, 
                       output_collector,
                       prefix):
        #self.distributed_inmemory_dataunit=distributed_inmemory_dataunit
        self.pilot=pilot
        self.compute_units=compute_units
        self.output_collector=output_collector
        self.prefix=prefix
        
    def cancel(self):
        """Attempt to cancel the call. If the call is currently being executed and cannot be 
           cancelled then the method will return False, otherwise the call will be 
           cancelled and the method will return True."""
        for cu in self.compute_units:
            cu.cancel()
       

    def get_state(self):
        """ get state of current pilot-based processing, i.e. the map resp. reduce phase"""
        pass
    
    
    def result(self): 
        """Return the value returned by the call. If the call hasn’t yet completed then this 
           method will wait for the result """
        logger.debug("Future: Wait for %d Compute Units"%(len(self.compute_units)))  
        for cu in self.compute_units:
            cu.wait()
        
        logger.debug("Future: Get output for %s: "%(self.prefix))
        #dus = self.distributed_inmemory_dataunit._get_output_du(self.prefix)
        result = self.output_collector(self.prefix)
        return result





class DistributedInMemoryDataUnit(object):  
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
        data = list(data)        
        self.len = len(data)
        part_start = 0
        part_end = self.len
        maxChunkSize = 1000000
        
        while part_end-part_start > 0:
            if part_end-part_start > maxChunkSize:
                logger.debug("Loading %s-%s" %( part_start, part_start+maxChunkSize))
                self.pipe.rpush(self.name, *data[part_start:part_start+maxChunkSize])
                part_start = part_start + maxChunkSize
            else:
                logger.debug("Loading last part %s-%s" % (part_start, part_end))
                self.pipe.rpush(self.name, *data[part_start:part_end])            
                part_start = part_end
            self.pipe.execute()                           

    
    def reload(self, data=[]):
        self.pipe.delete(self.name)
        self.len=0
        self.load(data)        
        
        
    def map_pilot(self, function, args, 
                  partition_prefix="map-part",
                  number_of_compute_units=1,
                  number_of_cores_per_compute_unit=1):        
        """ Execute map function using a set of CUs on the Pilot 
            TODO: add partitioning
        """
        prefix = partition_prefix + "-" + str(uuid.uuid4())[:8]
        number_of_lines_per_du=self.len/number_of_compute_units
        if self.len%number_of_compute_units>0:
            number_of_lines_per_du= number_of_lines_per_du + 1
        partition_start = 0
        cus=[]
        for i in range(0, number_of_compute_units):
            if partition_start + number_of_lines_per_du < self.len:
                partition_end = partition_start + number_of_lines_per_du
            else:
                partition_end = self.len
            logger.debug("CU %d:, Partition Start: %d, Partition End: %d"%(i,partition_start, partition_end))
            # start compute unit
            compute_unit_description = {
                "executable": "python",
                "arguments": ["-m", "distributed_inmem.dataunit", 
                              "-n", self.name,  
                              "-m", "kmeans.kmeans", 
                              "--map_function", function, 
                              "--partition_start", partition_start,
                              "--partition_end", partition_end,
                              "--args",  args, 
                              "--output_du_prefix", prefix],
                "number_of_processes": number_of_cores_per_compute_unit,
                "output": "stdout.txt",
                "error": "stderr.txt", 
            }
            compute_unit = self.pilot.submit_compute_unit(compute_unit_description)
            cus.append(compute_unit)
            partition_start = partition_end + 1
        #compute_unit.wait()
        
        future = Future(self.pilot, cus, self._get_output_du, prefix)
        return future
    
        
    def reduce_pilot(self, function, 
                     partition_prefix="reduce-part",
                     number_of_cores_per_compute_unit=1
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
            "number_of_processes": number_of_cores_per_compute_unit,
            "output": "stdout.txt",
            "error": "stderr.txt", 
        }
        compute_unit = self.pilot.submit_compute_unit(compute_unit_description)
        
        future = Future(self.pilot, [compute_unit], self._get_reduce_output, prefix)
        return future
        

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
    # Private
    def _get_output_du(self, prefix):
        names=self.redis_client.keys(prefix+"*")
        dus = []
        for n in names:
            du = DistributedInMemoryDataUnit(n, pilot=self.pilot)
            dus.append(du)
        return dus
    
    def _get_reduce_output(self, prefix):
        dus = self._get_output_du(prefix)
        return dus[0]
   



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
        map_reduce_result = du.map(function_pointer, args, int(parsed_arguments.partition_start), int(parsed_arguments.partition_end))
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
    
   

    
