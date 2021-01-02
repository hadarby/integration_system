import logging

from confluent_kafka import Producer

from utils.kafka_utils import KafkaUtils


class RecommendationsAmount:
    def __init__(self, recommendations_amount_topic, ratio=2, buffer=0):
        self._producer = Producer({"bootstrap.servers": "",
                                   "security.protocol": "SASL_SSL",
                                   "sasl.mechanisms": "PLAIN",
                                   "sasl.username": "",
                                   "sasl.password": ""})
        self._recommendations_amount_topic = recommendations_amount_topic
        self._ratio = ratio
        self._buffer = buffer

    def send_recommendations_amount(self, message):
        """
        Calculate the amounts of leads (AL) using the formula: AL = AA*RAT - (BUF + DIAL)
        For that we need dialer current statistics of:
        	DIAL - Amount of leads being dialed at this moment but are not connected to agents
        	AA - Available agents that are not connected to any call
        	RAT - The campaign current dial ratio.
        """
        try:
            amount = message['available_agents'] * self._ratio - (self._buffer + message['dial'])
            logging.info('Requesting for {} leads'.format(amount))
            self._producer.produce(self._recommendations_amount_topic, str(amount), callback=KafkaUtils.self.delivery_report)
            self._producer.poll(0)
        except Exception as ex:
            logging.info(
                "Exception while calculating and sending recommendations amount for message {} - {}".format(message,
                                                                                                            ex))

    def terminate(self):
        self._producer.flush()
