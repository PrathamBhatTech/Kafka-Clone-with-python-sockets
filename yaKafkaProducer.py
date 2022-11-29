import socket
import sys
import os
import time
import logging


class YAKafkaProducer(object):
    def __init__(self, bootstrap_server):
        self.bootstrap_server = bootstrap_server



