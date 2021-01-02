import logging


class KafkaUtils:
    @staticmethod
    def delivery_report(err, msg):
        """
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().
        """
        if err is not None:
            logging.error('Message {} delivery failed: {}'.format(msg, err))
        else:
            logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
