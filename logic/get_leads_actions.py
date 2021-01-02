import logging
from abc import abstractmethod

from confluent_kafka import Consumer
from confluent_kafka import Producer

from utils.kafka_utils import KafkaUtils


class GetLeadsActions:
    def __init__(self, group, consumer_topic, producer_topic):
        self._producer = Producer({"bootstrap.servers": "",
                                   "security.protocol": "SASL_SSL",
                                   "sasl.mechanisms": "PLAIN",
                                   "sasl.username": "",
                                   "sasl.password": ""})
        self._consumer = Consumer({
            "bootstrap.servers": "",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "",
            "sasl.password": "",
            'group.id': group,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
        })
        self._consumer.subscribe([consumer_topic])
        self._producer_topic = producer_topic

    @abstractmethod
    def start(self):
        """
        Get leads actions from dialer. Send the actions to self.send_leads_actions()
        """
        pass

    def send_leads_actions(self, actions):
        try:
            self._producer.produce(self._producer_topic, actions, callback=KafkaUtils.self.delivery_report)
            self._producer.poll(0)
            logging.info("Sent lead actions to SendLeadActions")
        except Exception as ex:
            logging.exception(
                'Exception while trying to send message {} to topic {} - {}'.format(actions, self._producer_topic, ex))

    def terminate(self):
        self._producer.flush()
