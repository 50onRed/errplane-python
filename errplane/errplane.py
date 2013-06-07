import base64
from multiprocessing import Pool
import logging
import requests


class Errplane(object):
    def __init__(self, api_key, application_id, environment="development", workers=3):
        self.pool = Pool(workers)
        self.api_key = api_key
        self.application_id = application_id
        self.environment = environment

    def report(self, name, value=1, timestamp="now", context=None):
        item = dict(name=name, value=value, timestamp=timestamp, context=context)
        self.pool.apply_async(process, (item, self.application_id, self.environment, self.api_key))


def process(point, application_id, environment, api_key):
    logging.debug('Received data')
    logging.debug(str(point))
    try:
        payload_data = {
            'name': point['name'],
            'value': str(point['value']),
            'timestamp': point['timestamp'],
        }
        if point['context']:
            payload_data['context'] = ' ' + base64.b64encode(point['context'])
        else:
            payload_data['context'] = ''

        data = '{name} {value} {timestamp}{context}'.format(**payload_data)

        params = {'api_key': api_key}
        url_fmt = "https://apiv2.errplane.com/databases/{application_id}{environment}/points"
        url = url_fmt.format(application_id=application_id, environment=environment)

        logging.debug("[Errplane] POSTing data:")
        requests.post(url, data=data, params=params, headers={'Connection': 'close'})

    except Exception as e:
        logging.error("[Errplane] Caught exception while processing queue: " + e.message)