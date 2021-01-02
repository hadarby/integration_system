import logging

from logic.get_recommendations import GetRecommendations
from utils.logging_utils import LoggingUtils
from utils.optimalq_connector import OptimalQConnector


def main():
    LoggingUtils.setup_logs()

    email = ''
    password = ''
    optimalq_connector = OptimalQConnector(email, password)
    token = optimalq_connector.get_token()
    consumer_group = 'get_recommendations'
    pool_uid = ''
    snooze_seconds = 10
    recommendations_amount_topic = 'recommendations_topic'
    recommendations_topic = 'push_recommendations_topic'

    get_recommendations = GetRecommendations(token,
                                             optimalq_connector,
                                             pool_uid,
                                             snooze_seconds,
                                             consumer_group,
                                             recommendations_amount_topic,
                                             recommendations_topic)
    try:
        get_recommendations.start()
    except SystemExit:
        logging.info("GetRecommendationsGenesys instructed to stop.")
    except Exception as ex:
        logging.exception("Exception in GetRecommendationsGenesys! - {}".format(ex))

    if get_recommendations is not None:
        get_recommendations.terminate()

    logging.info("GetRecommendationsGenesys has terminated.")


if __name__ == '__main__':
    main()

