'''
Distributed In-Memory DataUnit (Implemented on Kafka)
Created on Jul 5, 2014

@author: Andre Luckow
'''
import redis
from pykafka import KafkaClient
import argparse
import urlparse
import os, sys, time
import logging
import types
import threading
import importlib
import itertools
import uuid
logger = logging.getLogger('DistributedInMemoryDataUnitKafka')
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
        """Return the value returned by the call. If the call hasn't yet completed then this
           method will wait for the result """
        logger.debug("Future: Wait for %d Compute Units"%(len(self.compute_units)))
        for cu in self.compute_units:
            cu.wait()
        
        logger.debug("Future: Get output for %s: "%(self.prefix))
        #dus = self.distributed_inmemory_dataunit._get_output_du(self.prefix)
        result = self.output_collector(self.prefix)
        return result




class DistributedInMemoryDataUnit(object):  
    """ In-Memory DU backed by Kafka can serve as a producer and a consumer of a Kafka topic
        DU is mapped to a topic with the same name in Kafka
    """
            
    def __init__(self, name="test-dimdu", url='zookeeper://localhost:2181', pilot=None, mini_batch_size=10):
        """Broker_URL: """
        #Connection to Broker
        url_host_port=urlparse.urlparse(url).netloc
        self.client = KafkaClient(zookeeper_hosts=url_host_port)
        self.topic = self.client.topics[name]
        self.consumer = self.topic.get_simple_consumer()
        self.producer = self.topic.get_sync_producer()

        # Instance variables
        self.pilot = pilot
        self.name=name
        self.data = []
        self.len = 0
        self.mini_batch_size=mini_batch_size

    def load(self, data=[]):
        data = list(data)        
        self.len = len(data)
        for message in data:
            self.producer.produce(message.strip())


    def reload(self, data=[]):
        self.len=0
        self.load(data)  


    def delete(self):
        pass


    def map_pilot(self, function, args, 
                  number_of_compute_units=None,
                  number_of_cores_per_compute_unit=1):        
        """ Execute map function using a set of CUs on the Pilot 
            TODO: add partitioning
        """
        partitions=self.topic.partitions
        if number_of_compute_units==None:
            number_of_compute_units=len(partitions)

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
                              "-c", self.df.redisHost,
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
            compute_unit = self.df.pilot.submit_compute_unit(compute_unit_description)
            cus.append(compute_unit)
            partition_start = partition_end + 1
        #compute_unit.wait()
        
        future = Future(self.df.pilot, cus, self._get_output_du, prefix)
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
            "arguments": ["-m", "distributed_inmem.dataunit_kafka",
                          "-n", self.name,  
                          "-c", self.df.redisHost,
                          "-m", "kmeans.kmeans", 
                          "--reduce_function", function,
                          "--output_du_prefix", prefix],
            "number_of_processes": number_of_cores_per_compute_unit,
            "output": "stdout.txt",
            "error": "stderr.txt", 
        }
        compute_unit = self.df.pilot.submit_compute_unit(compute_unit_description)
        
        future = Future(self.df.pilot, [compute_unit], self._get_reduce_output, prefix)
        return future
        

    def map(self, module_name, function_name, args=None, start=0, end=None):

        map_function=self.get_function_pointer(module_name=module_name, function_name=function_name)
        message_list = []
        for message in self.consumer:
            if message is not None:
                print message.offset, message.value
                message_list.append(message)

            if len(message_list)>self.mini_batch_size:
                break

        result = []


        for m in message_list:
            if args==None:
                result.append(map_function(m))
            # else:
            #     # check weather arg is an DU that needs to get loaded
            #     if type(args)==types.StringType and self.df.redis_client.exists(args):
            #         args = DistributedInMemoryDataUnit(name=args, coordination=self.df).export()
            #     if args.__class__.__name__==DistributedInMemoryDataUnit.__name__:
            #         args = args.export()
            #     result.append(function(m, args))
        return result


    def reduce(self, function, args):
        end = self.df.redis_client.llen(self.name)
        points = self.df.redis_client.lrange(self.name, 0, end)
        result = None
        if args==None:
            result = function(points)
        else: 
            # check weather arg is an DU that needs to get loaded
            if type(args)==types.StringType and self.df.redis_client.exists(args):
                args = DistributedInMemoryDataUnit(name=args, coordination=self.df).export()
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
        pass
     


    ###########################################################################
    # Private
    def _get_output_du(self, prefix):
        names=self.df.redis_client.keys(prefix+"*")
        dus = []
        for n in names:
            du = DistributedInMemoryDataUnit(name=n, coordination=self.df)
            dus.append(du)
        return dus
    
    def _get_reduce_output(self, prefix):
        dus = self._get_output_du(prefix)
        return dus[0]
   
    @staticmethod
    def get_function_pointer(module_name="distributed_inmem.dataunit_kafka", function_name=""):
        mod = importlib.import_module(module_name)
        classname = function_name.split(".")[0]
        functionname = function_name.split(".")[1]
        class_pointer = getattr(mod, classname)
        function_pointer = getattr(class_pointer, functionname)
        return function_pointer


if __name__ == '__main__':
    """ Initiate a DataUnit an run a map or reduce function on it 
    
       python -m distributed_inmem.dataunit -n Points -m kmeans.kmeans --map_function KMeans.closestPoint --args Centers --shuffle_du_prefix=part
    
    """
    print "Start worker task for distributed in-memory dataunit"
    parser = argparse.ArgumentParser(add_help=True, description="""DistributedInMemoryDataUnit Startup Utility""")
    
    parser.add_argument('--coordination', '-c', default="zookeeper://localhost:2818")
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
    kafka_url = parsed_arguments.coordination
    du = DistributedInMemoryDataUnit(name=name, url=kafka_url)
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
        map_reduce_result = du.map(module, map_function, args, int(parsed_arguments.partition_start), int(parsed_arguments.partition_end))
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
