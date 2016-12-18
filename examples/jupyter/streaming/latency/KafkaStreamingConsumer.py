
# coding: utf-8

# # Streaming Consumer for Measuring Latency

# In[1]:

from pykafka import KafkaClient
import numpy as np
#import msgpack
#import msgpack_numpy as m
#m.patch()
import time
import datetime
import dateutil.parser


zkKafka='c251-104.wrangler.tacc.utexas.edu:2181'
client = KafkaClient(zookeeper_hosts=zkKafka)
#client = KafkaClient(hosts='c251-142.wrangler.tacc.utexas.edu:9092')
topic = client.topics['latency']
producer = topic.get_sync_producer()
consumer = topic.get_simple_consumer()


# In[5]:

run_timestamp=datetime.datetime.now()
RESULT_FILE= "results/kafka-latency-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
try:
    os.makedirs("results")
except:
    pass
output_file=open(RESULT_FILE, "w")


source_time=1482011522.371088
sink_time=1482011549.907337
TIME_OFFSET=sink_time-source_time

while True:
    message = consumer.consume(block=True)
    #print message.value
    now = time.time()
    #sent_time=datetime.datetime.strptime(message.value, "%Y-%m-%dT%H:%M:%S.%fZ")
    sent_time_string = message.value.split(";")[0]
    sleep_time =float(message.value.split(";")[1])
    sent_time = dateutil.parser.parse(sent_time_string)
    sent_time_ts = time.mktime(sent_time.timetuple())
    lat = now-sent_time_ts - TIME_OFFSET   
    result = "kafka, latency, 0, %.5f, %.5f, %s, %s\n"%(1/sleep_time, lat, message.value.split(";")[0], 
                                                                    datetime.datetime.now().isoformat())
    print(result)
    output_file.write(result)
    output_file.flush()


# In[13]:




# In[12]:




# In[ ]:



