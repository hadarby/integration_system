import requests
import logging


class OptimalQConnector:
    def __init__(self, email, password):
        self.url = 'https://sso.storky.optimalq.net/v1/authenticate'
        self.data = {'email': email, 'password': password}

    def get_token(self):
        success_get_token = requests.post(url=self.url, json=self.data, headers={'Content-Type': 'application/json'})
        code = success_get_token.status_code
        counter = 5
        while (counter > 0) and ((code < 200) or (code > 299)):
            logging.error('Connection to OptimalQ failed. code : {} attempt {}/5'.format(code, 6-counter))
            counter -= 1
            success_get_token = requests.post(url=self.url, json=self.data, headers={'Content-Type': 'application/json'})
            code = success_get_token.status_code
        if (code > 199) and (code < 300):
            logging.info('Got token')
            return success_get_token.json()["response"]["token"]
        logging.error('Connection to OptimalQ failed. code : {}'.format(code))
        return None

