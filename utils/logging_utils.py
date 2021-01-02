import logging
import sys


class LoggingUtils:
    @staticmethod
    def setup_logs(log_level=logging.INFO):
        """
        Set up the logs according to the given level
        :param log_level: The level of logs to set
        """
        root = logging.getLogger()
        root.setLevel(log_level)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)