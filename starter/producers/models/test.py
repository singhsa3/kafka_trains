import pip
from producer import  Producer
import logging
from pathlib import Path
import json

from confluent_kafka import avro

def import_or_install(package):
    try:
        __import__(package)
    except ImportError:
        pip.main(['install', package])

import_or_install("pandas")

key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")

p = Producer("hello",key_schema,1,1)
p.close()

from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': "host1:9092,host2:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)

producer.produce("dads", key="key", value="value")
producer.
