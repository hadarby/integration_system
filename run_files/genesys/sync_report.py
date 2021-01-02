import logging

from logic.sync_report import SyncReport
from utils.logging_utils import LoggingUtils
from utils.optimalq_connector import OptimalQConnector


def main():
    LoggingUtils.setup_logs()
    email = ''
    password = ''
    optimalq_connector = OptimalQConnector(email, password)
    token = optimalq_connector.get_token()
    group = "sync_report"
    pool_uid = ""
    call_reports_topic = 'call_report_topic'
    sync_report = SyncReport(group, token, optimalq_connector, pool_uid, call_reports_topic)

    try:
        sync_report.start()
    except SystemExit:
        logging.info("SyncReport instructed to stop.")
    except Exception as ex:
        logging.exception("Exception in SyncReport! - {}".format(ex))

    if sync_report is not None:
        sync_report.terminate()

    logging.info("SyncReport has terminated.")


if __name__ == '__main__':
    main()
