import json
import logging
from abc import abstractmethod

from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer


class PushRecommendations:

    def __init__(self, group, recommedations_topic):
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
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest'
        })

        self._consumer.subscribe([recommedations_topic])

    def start(self):
        """
        Get messages from push_recommendations_topic.
        If we get recommendations then call self.push_recommendations()
        :return:
        """
        while True:
            msg = self._consumer.poll(0.1)

            if msg is None:
                continue
            elif not msg.error():  # Received message
                recommendations = msg.value()
                recommendations = json.loads(recommendations)

                if len(recommendations['response']['leads']) == 0:
                    logging.error('Got 0 optimal leads from OptimalQ')
                    continue

                self.push_recommendations(recommendations)
                self._consumer.commit()
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info('End of partition reached {}/{}'.format(msg.topic(), msg.partition()))
            else:
                logging.error('Error occurred: {}'.format(msg.error().str()))

    @abstractmethod
    def push_recommendations(self, recommendations):
        """
        Get OptimalQ recommendations and push them to the dialer, depends on dialer api options.
        :param recommendations:
        """
        pass

    def terminate(self):
        self._producer.flush()

