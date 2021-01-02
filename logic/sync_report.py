import logging

import requests
from confluent_kafka import Consumer, KafkaError


class SyncReport:

    def __init__(self, group, token, optimalq_connector, pool_uid, call_reports_topic):
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
        self._consumer.subscribe([call_reports_topic])
        self._headers = {"X-Auth-Token": "{}".format(token), "Content-Type": "application/json"}
        self._optimalq_connector = optimalq_connector
        self._pool_uid = pool_uid
        self._optimalq_url = ''


    def start(self):
        """
        Get messages from call_report_topic.
        Send the call reports to post_call_report
        """
        while True:
            msg = self._consumer.poll(0.1)
            if msg is None:
                continue
            elif not msg.error(): #Received message
                self.post_call_report(msg.value())
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info('End of partition reached {}/{}'.format(msg.topic(), msg.partition()))
            else:
                logging.error('Error occurred: {}'.format(msg.error().str()))

    def post_call_report(self, call_report):
        """
        Post call report json to OptimalQ API by pool uid
        :param call_report:
        :return:
        """
        url = '{}/v1/pools/{}/call_reports'.format(self._optimalq_url, self._pool_uid)
        success_post_call_report = requests.post(url=url, data=call_report, headers=self._headers)
        code = success_post_call_report.status_code
        counter = 5

        while (counter > 0) and ((code < 200) or (code > 299)):
            counter -= 1
            token = self._optimalq_connector.get_token()

            if token is not None:
                self._headers = {"X-Auth-Token": "{}".format(token), "Content-Type": "application/json"}

            success_post_call_report = requests.post(url=url, data=call_report, headers=self._headers)
            code = success_post_call_report.status_code

        if (code > 199) and (code < 300):
            self._consumer.commit()
            logging.info('Sent call report for pool: {}'.format(self._pool_uid))
            return

        logging.error('Connection to OptimalQ failed while trying to send call report {}. code: {}, error: {}'.format(
            self._pool_uid, code, success_post_call_report.content))

    def terminate(self):
        self._consumer.close()
