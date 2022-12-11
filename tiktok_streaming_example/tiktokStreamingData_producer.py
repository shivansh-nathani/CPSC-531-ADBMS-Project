#!/usr/bin/env python

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import numpy as np             
from sys import argv, exit
from time import time, sleep

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()          # reading config file that has connection details of kafka cloud
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    producer = Producer(config)     # creating object of confluent_kafka producer

   
    def delivery_callback(err, msg):        # callback method to be called once message is pushed to kafka topic
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    topic = "tiktok_category"       # defining topic to push data to
   
    categories = ['Entertainment','Dance','Pranks','Fitness/sports','Home reno/DIY','Beauty/skin care','Fashion','Cooking/recipes']     # sample tiktok categories to mock data from tiktok as no open api is present
    maxDuration = 30   # defining max tiktok video duration to mock 
    like = (0,1)
    count = 0
    while True:
        userId = np.random.randint(5000000,5000010)         # producing data for random user 
        print(userId)
        category = choice(categories)                       # selecting random category to mock user viewed video details
        watchTime = np.random.randint(0,maxDuration)        # selecting random watchtime to mock user viewed video details
        liked = choice(like)                                # randomly selecting if user liked the video or no to mock user viewed video details
        msg = f'{time()},{category},{watchTime},{liked}'    # formatting all of the mock data to be pushed to kafka topic
        print(msg)                      
        producer.produce(topic, msg,str(userId), callback=delivery_callback)        # pushing data to kafka topic using produce method of Producer class
        count += 1
        sleep(3.0)
        producer.poll(10000)
        producer.flush()

