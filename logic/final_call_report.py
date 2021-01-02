import logging
from abc import abstractmethod

from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer

from utils.kafka_utils import KafkaUtils


class FinalCallReport:
    """
    If CRM system send two reports per call CallReport class will handle thw first one.
    FinalCallReport will handle the final report.
    """

    def __init__(self, group=None, consumer_topic=None, producer_topic=None):
        self.producer = Producer({"bootstrap.servers": "",
                                  "security.protocol": "SASL_SSL",
                                  "sasl.mechanisms": "PLAIN",
                                  "sasl.username": "",
                                  "sasl.password": ""})
        self._consumer = Consumer({
            "bootstrap.servers": "",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms":"PLAIN",
            "sasl.username": "",
            "sasl.password": "",
            'group.id': group,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
        })
        self._consumer.subscribe([consumer_topic])
        self._producer_topic = producer_topic

    @abstractmethod
    def get_call_report(self):
        """
        Implementation for stream case
        """
        while True:
            msg = self._consumer.poll(0.1)

            if msg is None:
                continue

            elif not msg.error():  # Received message
                self._consumer.commit()
                self.send_call_report(msg.value())
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info('End of partition reached {}/{}'.format(msg.topic(), msg.partition()))
            else:
                logging.error('Error occurred: {}'.format(msg.error().str()))

    def send_call_report(self, report):
        """
        Depends on dialer API options
        :param report:
        """
        while True:
            self.producer.produce(self._producer_topic, report, callback=KafkaUtils.self.delivery_report)
            self.producer.poll(0)

    def terminate(self):
        self.producer.flush()

