import logging

from logic.genesys.push_recommendations import PushRecommendationsGenesys
from utils.logging_utils import LoggingUtils


def main():
    LoggingUtils.setup_logs()
    group = "push_recommendations"
    recommendations_topic = 'push_recommendations_topic'
    push_recommendations = PushRecommendationsGenesys(group, recommendations_topic)

    try:
        push_recommendations.start()
    except SystemExit:
        logging.info("PushRecommendationsGenesys instructed to stop.")
    except Exception as ex:
        logging.exception("Exception in PushRecommendationsGenesys! - {}".format(ex))

    if push_recommendations is not None:
        push_recommendations.terminate()

    logging.info("PushRecommendationsGenesys has terminated.")


if __name__ == '__main__':
    main()
