import time
import json
import uuid
import random
from typing import NamedTuple, Tuple
from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType, SimpleConsumer
from pykafka.producer import Producer


class ClientConf(NamedTuple):
    host: str
    port:str
    topic_name: str

class Client():
    
    def __init__(self, config: ClientConf):
        self.kafka_url = "{host}:{port}".format(host=config.host, port=config.port)
        self.topic_name = config.topic_name
        self.client = KafkaClient(hosts=self.kafka_url)

    def getProducer(self) -> Producer:
        topic = self.client.topics[self.topic_name]
        # print(topic)
        return topic.get_sync_producer()
        
    def getConsumer(self) -> SimpleConsumer:
        topic = self.client.topics[self.topic_name]
        return topic.get_simple_consumer(
            consumer_group="mygroup",
            auto_offset_reset=OffsetType.EARLIEST,
            reset_offset_on_start=False
            )
