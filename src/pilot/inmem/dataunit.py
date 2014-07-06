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
logger = logging.getLogger('DistributedInMemoryDataUnit')


class PilotManager(object):

    @staticmethod
    def start_pilot(self, pilot_compute_description=None):
        COORDINATION_URL = "redis://localhost:6379"
        pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)
        if pilot_compute_description==None:
            pilot_compute_description = {
                                 "service_url": 'fork://localhost',
                                 "number_of_processes": 2,                             
                                 "working_directory": os.getcwd() + "/work/",
                                 }    
        pilot = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
        return pilot


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
        self.pipe.execute()
    
    def reload(self, data=[]):
        self.pipe.delete(self.name)
        for i in data:
            self.pipe.rpush(self.name, i)
        self.pipe.execute()
        
    def map_pilot(self, function, args):
        # start compute unit
        compute_unit_description = {
            "executable": "python",
            "arguments": ["-m", "pilot.inment.dataunit.DistributedInMemoryDataUnit", self.name],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt", 
        }
        compute_unit = self.pilot.submit_compute_unit(compute_unit_description)
        
        

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
    distributed_data_unit = DistributedInMemoryDataUnit()
    parser = argparse.ArgumentParser(add_help=True, description="""DistributedInMemoryDataUnit Startup Utility""")
    
    parser.add_argument('--coordination', '-c', default="redis://localhost")
    parser.add_argument('--password', '-p', default="")
    parser.add_argument('--name', '-n')    
    parser.add_argument('--module', '-m')
    parser.add_argument('--function', '-f')    
    parser.add_argument('--args', '-a')        
    
    parsed_arguments = parser.parse_args()  
    