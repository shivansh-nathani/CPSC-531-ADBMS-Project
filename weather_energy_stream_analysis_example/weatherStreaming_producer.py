import requests

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import numpy as np             
from sys import argv, exit
from time import time, sleep,ctime

def getWeatherData(city):
    url = "https://weatherapi-com.p.rapidapi.com/current.json"

    querystring = {"q":city}

    headers = {
        "X-RapidAPI-Key": "90de3c61e8msh4e580b2d10c0fd4p17684bjsnead61cb7e42b",
        "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response.json()



#!/usr/bin/env python


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

   
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    cities = ['Fullerton US','Los Angeles US','Pune India','Mumbai India']
    topic = "weather"
    while True:
        for city in cities:
            msg = f'{getWeatherData(city)}'
            print(msg)
            producer.produce(topic, msg,str(city), callback=delivery_callback)
            producer.poll(10000)
            producer.poll()
            producer.flush()
        sleep(300.0)


