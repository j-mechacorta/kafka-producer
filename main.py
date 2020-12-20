from typing import Tuple
import concurrent.futures
import threading
import time
import json
import uuid
import random
import logging
from kafka.client import ClientConf, Client


my_kafka_conf = ClientConf('localhost','9092','massive-events')
kafka_client = Client(my_kafka_conf)
consumer = kafka_client.getConsumer()
my_threads=[]

# producer_parmas: Tuple = (2, True)

def produceMessages(producer_parmas: tuple):
    logging.info("number_of_messages: %s, endless: %s", producer_parmas[0], producer_parmas[1])
    number_of_messages = producer_parmas[0]
    endless = producer_parmas[1]
    index = 0
    producer = kafka_client.getProducer()
    x = threading.currentThread().ident
    logging.info("Producer Initialized - Thread_id: %s", threading.currentThread().ident)
    # my_threads = my_threads.append(threading.currentThread().name)
    my_threads.append(x)
    print(my_threads)
    while index < number_of_messages: 
        message =  {'id':  str(uuid.uuid4()), 'desc': f'test message: {x}' }
        logging.debug("Thread sending messag: %s", json.dumps(message))
        bmessage = bytes(json.dumps(message).encode('utf-8'))
        producer.produce(bmessage)
        if not endless:
            index += 1

def checkThreadStatus():
    logging.info("Status Check - Thread Initialized - Thread_id: %s", threading.currentThread().ident)
    
    while True:
        time.sleep(5)
        logging.info("currently registered threads: %s", my_threads)
        
    # for thread in threading.enumerate():
    #     logging.info("these are the threads: %s", thread.ident)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.getLogger().setLevel(logging.INFO)

    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    logging.info("Initializing")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(produceMessages, [(1,True),(1,True),(1,True)])
        