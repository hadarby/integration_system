import json
import logging
from abc import abstractmethod

from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer

from utils.kafka_utils import KafkaUtils


class CallReport:
    """
    If the CRM system send two reports per call this class will handle the first one.
    FinalCallReport will handle the final report.
    """

    def __init__(self, group=None, consumer_topic=None, producer_topic=None):
        self._producer = Producer({"bootstrap.servers": "",
                                   "security.protocol": "SASL_SSL",
                                   "sasl.mechanisms": "PLAIN",
                                   "sasl.username": "",
                                   "sasl.password": ""})
        self._group = group
        self._consumer_topic = consumer_topic
        self._consumer = None
        self._producer_topic = producer_topic

    @abstractmethod
    def start(self):
        """
        Example implementation for stream case
        """
        self._consumer = Consumer({
            "bootstrap.servers": "",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms":"PLAIN",
            "sasl.username": "",
            "sasl.password": "",
            'group.id': self._group,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
        })
        self._consumer.subscribe([self._consumer_topic])

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

    @abstractmethod
    def manage_call_report(self, call_report):
        """
        this function will be overwritten per setup on the dialer.
        The goal is to reformat the call report into the json format for OptimalQ's post request
        :param call_report:
        :return: call post_call_report(sample_pool_id, call_report)
        """
        pass

    def send_call_report(self, report):
        """
        Sends call report to the SyncReport service via kafka
        :param report:
        :return:
        """
        try:
            report = json.dumps(report)
            self._producer.produce(self._producer_topic, report, callback=KafkaUtils.self.delivery_report)
            self._producer.poll(0)
            logging.info("Sent call report to SyncReport")
        except Exception as ex:
            logging.exception(
                'Exception while trying to send message {} to topic {} - {}'.format(report, self._producer_topic, ex))

    def terminate(self):
        self._producer.flush()

