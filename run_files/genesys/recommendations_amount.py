import asyncio
import logging

from logic.genesys.recommendations_amount import RecommendationsAmountGenesys
from utils.logging_utils import LoggingUtils


def main():
    LoggingUtils.setup_logs()
    recommendations_amount_topic = 'recommendations_topic'
    recommendations_amount = RecommendationsAmountGenesys(recommendations_amount_topic)

    try:
        grouped_async = asyncio.gather(recommendations_amount.start())
        asyncio.get_event_loop().run_until_complete(grouped_async)
    except SystemExit:
        logging.info("RecommendationsAmountGenesys instructed to stop.")
    except Exception as ex:
        logging.exception("Exception in RecommendationsAmountGenesys! - {}".format(ex))

    if recommendations_amount is not None:
        recommendations_amount.terminate()

    logging.info("RecommendationsAmountGenesys has terminated.")


if __name__ == '__main__':
    main()
