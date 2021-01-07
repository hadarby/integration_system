from logic.genesys.recommendations_amount import RecommendationsAmountGenesys
import logging
import sys
import asyncio


def main():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    recommendations_amount = RecommendationsAmountGenesys()
    try:
        recommendations_amount.start()
    except SystemExit:
        logging.info("RecommendationsAmountGenesys instructed to stop.")
    except Exception as ex:
        logging.exception("Error in RecommendationsAmountGenesys! - {}".format(ex))
    recommendations_amount.terminate()
    logging.info("RecommendationsAmountGenesys has terminated.")


if __name__ == '__main__':
    main()
