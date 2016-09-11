import os
import sys
import pickle
import time
import datetime
import logging
import urllib, json
import socket


def get_kafka_config_details(working_directory=None, all=False):
    base_work_dir = os.path.join(working_directory, "work")
    kafka_config_dirs = [i if os.path.isdir(os.path.join(base_work_dir,i)) and i.find("kafka-")>=0 else None for i in 
                         os.listdir(base_work_dir)]
    kafka_config_dirs = filter(lambda a: a != None, kafka_config_dirs)
    kafka_config_dirs.sort(key=lambda x: os.path.getmtime(os.path.join(base_work_dir, x)),  reverse=True)
    if all == False: kafka_config_dirs=kafka_config_dirs[:1]
    brokers = []
    zk = ""
    for kafka_config_dir in kafka_config_dirs:
        conf = os.path.join(base_work_dir, kafka_config_dir, "config")
        broker_config_dirs =[i if os.path.isdir(os.path.join(conf, i)) and i.find("broker-")>=0 else None for i in 
                             os.listdir(conf)]
        broker_config_dirs = filter(lambda a: a != None, broker_config_dirs)
        for broker in broker_config_dirs:
            with open(os.path.join(conf,broker, "server.properties"), "r") as config:
                print "Kafka Config: %s (%s)"%(conf, time.ctime(os.path.getmtime(conf)))
                lines = config.readlines()
                for line in lines:
                    if line.startswith("listeners"):
                        broker_url = line.split("=")[1].strip() # parse listeners=PLAINTEXT://c251-135:9092
                        broker_url = broker_url[broker_url.index("://")+3:] # remove PLAINTEXT://
                        brokers.append(broker_url)
                    elif line.startswith("zookeeper.connect="):
                        zk=line.split("=")[1].strip()                                       
    return (brokers, zk)


def get_spark_master(working_directory=None):
    # search for spark_home:
    print working_directory
    base_work_dir = os.path.join(working_directory, "work")
    #spark_home=''.join([i.strip() if os.path.isdir(os.path.join(base_work_dir, i)) and i.find("spark")>=0 else '' for i in 
    #                    os.listdir(base_work_dir)])
    #spark_home_path=os.path.join(working_directory, "work", os.path.basename(spark_home))
    master_file=os.path.join(base_work_dir, "spark_master")
    print "Search for: " + master_file
    counter = 0
    while os.path.exists(master_file)==False and counter <600:
        time.sleep(1)
        counter = counter + 1

    with open(master_file, 'r') as f:
        master = f.read().strip()

    return master

