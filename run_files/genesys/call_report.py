import logging

from logic.genesys.call_report import CallReportGenesys
from utils.logging_utils import LoggingUtils


def main():
    LoggingUtils.setup_logs()
    call_report = CallReportGenesys()

    try:
        call_report.start()
    except SystemExit:
        logging.info("CallReportGenesys instructed to stop.")
    except Exception as ex:
        logging.exception("Exception in CallReportGenesys! - {}".format(ex))

    if call_report is not None:
        call_report.terminate()

    logging.info("CallReportGenesys has terminated.")


if __name__ == '__main__':
    main()

