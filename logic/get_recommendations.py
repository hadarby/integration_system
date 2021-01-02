import json
import logging

import requests
from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer

from utils.kafka_utils import KafkaUtils


class GetRecommendations:

    def __init__(self,
                 token,
                 optimalq_connector,
                 pool_uid,
                 snooze_seconds,
                 consumer_group,
                 recommendations_amount_topic,
                 recommendations_topic):
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
            'group.id': consumer_group,
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest'
        })
        self._consumer.subscribe([recommendations_amount_topic])
        self._headers = {"X-Auth-Token": "{}".format(token), "Content-Type": "application/json"}
        self._optimalq_connector = optimalq_connector
        self._pool_uid = pool_uid
        self._snooze_seconds = snooze_seconds
        self._optimalq_url = ''
        self._recommendations_topic = recommendations_topic

    def start(self):
        """
        Get messages from recommendations_topic.
        Send the leads actions to self.get_optimal()
        """
        while True:
            msg = self._consumer.poll(0.1)

            if msg is None:
                continue
            elif not msg.error():  # Received message
                self.get_optimal(msg.value())
                self._consumer.commit()
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info('End of partition reached {}/{}'.format(msg.topic(), msg.partition()))
            else:
                logging.error('Error occurred: {}'.format(msg.error().str()))

    def get_optimal(self, amount):
        """
        Get Optimal leads from OptimalQ API and send them to self.send_optimal_recommendations()
        :param amount:
        """
        try:
            amount = int(amount)
            url = '{}/v1/pools/{}/leads/optimal?count={}&SnoozeSeconds={}'\
                .format(self._optimalq_url, self._pool_uid, amount, self._snooze_seconds)
            optimal_results = requests.get(url, headers=self._headers)
            code = optimal_results.status_code
            counter = 5

            while (counter > 0) and ((code < 200) or (code > 299)):
                counter -= 1
                token = self._optimalq_connector.get_token()

                if token is not None:
                    self._headers = {"X-Auth-Token": "{}".format(token), "Content-Type": "application/json"}

                optimal_results = requests.get(url, headers=self._headers)
                code = optimal_results.status_code

            if (code > 199) and (code < 300):
                logging.info('Get optimal leads for pool: {}'.format(self._pool_uid))
                self.send_optimal_recommendations(optimal_results)
                return

            logging.error(
                'Connection to OptimalQ failed while trying to get {} optimal leads for pool {}, code: {}, error: {}'.format(
                    amount, self._pool_uid, code, optimal_results.content))
        except Exception as ex:
            logging.exception("Exception while getting {} optimalq leads from OptimalQ - {}".format(amount, ex))

    def send_optimal_recommendations(self, recommendations):
        self._producer.produce(self._recommendations_topic, json.dumps(recommendations.json()), callback=KafkaUtils.self.delivery_report)
        self._producer.poll(0)

    def terminate(self):
        self._producer.flush()

