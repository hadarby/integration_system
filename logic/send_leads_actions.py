import logging

import requests
from confluent_kafka import Consumer, KafkaError


class SendLeadsActions:

    def __init__(self, group, token, optimalq_connector, pool_uid, lead_actions_topic):
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
        self._consumer.subscribe([lead_actions_topic])
        self._headers = {"X-Auth-Token": "{}".format(token), "Content-Type":"application/json"}
        self._optimalq_connector = optimalq_connector
        self._pool_uid = pool_uid
        self._optimalq_url = ''

    def start(self):
        """
        Get messages from leads_actions_topic.
        Send the leads actions to self.manage_leads_actions()
        """
        while True:
            msg = self._consumer.poll(0.1)

            if msg is None:
                continue
            elif not msg.error():  # Received message
                self.manage_leads_actions(msg.value())
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info('End of partition reached {}/{}'.format(msg.topic(), msg.partition()))
            else:
                logging.error('Error occurred: {}'.format(msg.error().str()))

    def manage_leads_actions(self, leads_actions):
        """
        This function will be overwritten per setup on the dialer.
        The goal is to format the leads in to json format for OptimalQ's post request
        On call use the right function depending on the action get / put (update) / post / delete
        :param leads_actions:
        :return: call post/put/get/delete_leads(leads_actions)
        """
        pass

    def post_leads(self, leads_actions):
        """
        Add leads to pool
        :param leads_actions:
        :return: True (on success) / False
        """
        url = '{}/v1/pools/{}/leads'.format(self._optimalq_url, self._pool_uid)
        success_post_leads = requests.post(url=url, data=leads_actions, headers=self._headers)
        code = success_post_leads.status_code
        counter = 5

        while (counter > 0) or ((code < 200) and (code > 299)):
            counter -= 1
            if (code > 299) and (code < 400):
                token = self._optimalq_connector.get_token()

                if token is not None:
                    self._headers = {"X-Auth-Token": "{}".format(token), "Content-Type":"application/json"}

            success_post_leads = requests.post(url=url, data=leads_actions, headers=self._headers)
            code = success_post_leads.status_code
        if (code > 199) and (code < 300):
            self._consumer.commit()
            logging.info('Added leads to pool: {}'.format(self._pool_uid))

            return True

        logging.error('Connection to OptimalQ failed while trying to add leads to pool {}'.format(self._pool_uid))

        return False

    def put_leads(self, leads_actions):
        """
        Set leads to pool
        :param leads_actions:
        :return: True (on success) / False
        """
        url = '{}/v1/pools/{}/leads'.format(self._optimalq_url, self._pool_uid)
        success_post_leads = requests.put(url=url, data=leads_actions, headers=self._headers)
        code = success_post_leads.status_code
        counter = 5

        while (counter > 0) or ((code < 200) and (code > 299)):
            counter -= 1

            if (code > 299) and (code < 400):
                token = self._optimalq_connector.get_token()

                if token is not None:
                    self._headers = {"X-Auth-Token": "{}".format(token), "Content-Type":"application/json"}

            success_post_leads = requests.put(url=url, data=leads_actions, headers=self._headers)
            code = success_post_leads.status_code

        if (code > 199) and (code < 300):
            self._consumer.commit()
            logging.info('Set leads to pool: {}'.format(self._pool_uid))

            return True

        logging.error('Connection to OptimalQ failed while trying to set leads to pool {}'.format(self._pool_uid))

        return False

    def delete_leads(self, leads_actions):
        """
        Delete leads from pool
        :param leads_actions:
        :return: True (on success) / False
        """
        url = '{}/v1/pools/{}/leads'.format(self._optimalq_url, self._pool_uid)
        success_post_leads = requests.delete(url=url, data=leads_actions, headers=self._headers)
        code = success_post_leads.status_code
        counter = 5

        while (counter > 0) and ((code < 200) or (code > 299)):
            counter -= 1

            if (code > 299) and (code < 400):
                token = self._optimalq_connector.get_token()

                if token is not None:
                    self._headers = {"X-Auth-Token": "{}".format(token), "Content-Type":"application/json"}

            success_post_leads = requests.delete(url=url, data=leads_actions, headers=self._headers)
            code = success_post_leads.status_code
        if (code > 199) and (code < 300):
            self._consumer.commit()
            logging.info('Deleted leads from pool: {}'.format(self._pool_uid))

            return True

        logging.error('Connection to OptimalQ failed while trying to delete leads from pool {}'.format(
                                                                                                self._pool_uid))
        return False

