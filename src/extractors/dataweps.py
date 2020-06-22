# get info on material we have on a product card / exists in sortiment rep
import datetime as dt
import logging
from datetime import datetime
from datetime import timedelta
from time import sleep
from traceback import format_tb

import pandas as pd
import pytz
import requests


class DatawepsProducer(object):
    def __init__(self, pipeline, datadir, parameters):
        self.datadir = datadir
        self.user = parameters.get('#user')
        self.password = parameters.get('#password')
        self.url = parameters.get('url')
        self.bulk_size = parameters.get('bulk_size') or 500
        self.out_cols = parameters.get('out_cols')
        self.source = parameters.get('source')
        new_timestamp = datetime.utcnow()
        self.ts = new_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        self.source_id = f'{self.source}_{new_timestamp.strftime("%Y%m%d%H%M%S")}'
        self.task_queue = pipeline

    # parser
    def parse(self, i):
        product = dict()
        product['TS'] = self.ts
        product['COUNTRY'] = 'CZ'
        product['DISTRCHAN'] = 'CZC'
        product['MATERIAL'] = i['internal_code']
        # product['cse_name'] = i['name']
        product['ESHOP'] = i['shop']
        product['SOURCE'] = self.source
        product['FREQ'] = 'd'
        product['URL'] = i['url']
        product['STOCK'] = 1 if i['stock'] is True else 0
        product['PRICE'] = i['price_vat']
        product['SOURCE_ID'] = self.source_id
        if 'availability' in i:
            # prefer global/eshop availability to regional
            availability = i['availability'].get('global') or i['availability'].get('eshop')
            # as a last resort, take first regions availability
            if availability is None and i['availability'].values():
                availability = list(i['availability'].values())[0]
            product['AVAILABILITY'] = availability

        return product

    def produce(self):
        url = self.url
        user = self.user
        pasw = self.password
        out_cols = self.out_cols

        # params
        per_page = self.bulk_size
        time_from = (dt.datetime.now(pytz.timezone('Europe/Prague')) + timedelta(hours=-7)).strftime(
            '%Y-%m-%dT%H:%M:%S+01:00')
        time_to = (dt.datetime.now(pytz.timezone('Europe/Prague')) + timedelta(hours=1)).strftime(
            '%Y-%m-%dT%H:%M:%S+01:00')

        payload = {'time_from': time_from, 'time_to': time_to, 'per_page': per_page}

        # get and parse response
        last_page = False
        lst = []

        scroll_limit = 0
        try:
            while last_page is False:
                # requests.get may raise timeout exception, let it end scraping
                r = requests.get(url, auth=(user, pasw), params=payload, timeout=120)
                resp = r.json()

                if resp.get('code') == 429:
                    # scroll request limit reached, try waiting and rerun last url request
                    scroll_limit += 1
                    if scroll_limit >= 25:
                        raise Exception('Scroll limit reached repeatedly')

                    sleep(1)
                    continue

                for i in resp['results']:
                    lst.append(self.parse(i))

                df = pd.DataFrame(lst, columns=out_cols)
                df['STOCK'] = df['STOCK'].fillna(0).astype(int)
                df.fillna('', inplace=True)
                # output to queue for writing
                self.task_queue.put(df.to_dict('records'))
                if resp['pagination']['last_page'] is False:
                    url = resp['pagination']['next_page']
                else:
                    last_page = True
        except Exception as e:
            trace = 'Traceback:\n' + ''.join(format_tb(e.__traceback__))
            logging.error(f'Error occured {str(e)}', extra={'full_message': trace})
        finally:
            # let writer know extracting is finished
            self.task_queue.put('DONE')
