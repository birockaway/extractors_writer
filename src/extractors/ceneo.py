import datetime
import itertools
import logging
import re
import time
from traceback import format_tb
from urllib.parse import urlparse

import requests

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def parse_offer(offer_raw):
    offer = offer_raw.copy()
    eshop = (urlparse(offer.get("CustName", "")).netloc.lower()
             if urlparse(offer.get("CustName", "")).netloc.lower() != ""
             else offer.get("CustName", "").lower()
             )
    offer["eshop"] = re.sub('(^www.)|(/*)', "", eshop)
    return offer


def parse_product(product_card):
    common_keys = {"product_" + k: v for k, v in product_card.items() if k != "offers"}
    common_keys["cse_url"] = f"https://ceneo.pl/{common_keys.get('product_CeneoProdID', '')}"

    if product_card.get("offers") is not None:
        offer_details = product_card["offers"]["offer"]
    else:
        return [common_keys]

    eshop_offers = [{**common_keys, **parse_offer(offer_detail)} for offer_detail in offer_details]
    return eshop_offers


def scrape_batch(url, key, batch_ids):
    params = (("apiKey", key),
              ("resultFormatter", "json"),
              ("shop_product_ids_comma_separated", ",".join(batch_ids))
              )
    try:
        req = requests.get(url, params=params)

    except Exception as e:
        logger.debug(f"Request failed. Exception {e}. Batch ids: {batch_ids}")
        return None

    else:
        if not req.ok:
            logger.debug(f"Request failed. Status: {req.status_code}")
            return None

        req_json = req.json()

        if req_json["Response"]["Status"] != "OK":
            logger.debug(f"Request body returned non-ok status. {req_json}. Batch ids: {batch_ids}")
            return None

        if "product_offers_by_ids" not in req_json["Response"]["Result"].keys():
            logger.debug(f"Request did not return any product data. Result: {req_json}. Batch ids: {batch_ids}")
            return None

        result = list(itertools.chain(*[parse_product(item) for item
                                        in req_json["Response"]["Result"]["product_offers_by_ids"][
                                            "product_offers_by_id"]]))

        return result


def batches(product_list, batch_size, sleep_time=0):
    prod_batch_generator = (
        (k, [prod_id for _, prod_id in g])
        for k, g
        in itertools.groupby(enumerate(product_list), key=lambda x_: x_[0] // batch_size)
    )

    # yield the first batch without waiting
    yield next(prod_batch_generator, (None, []))

    for batch in prod_batch_generator:
        time.sleep(sleep_time)
        # yield batch for processing
        yield batch


class CeneoProducer:
    def __init__(self, task_queue, datadir, parameters):
        self.task_queue = task_queue
        self.datadir = datadir
        self.parameters = parameters
        # log parameters (excluding sensitive designated by '#')
        logger.info({k: v for k, v in self.parameters.items() if "#" not in k})

    def parse_product_ids(self):
        kbc_datadir = self.datadir
        input_filename = self.parameters.get("input_filename")
        logger.warning(f'opening file: {kbc_datadir}in/tables/{input_filename}.csv')
        # read unique product ids
        with open(f'{kbc_datadir}in/tables/{input_filename}.csv') as input_file:
            logger.warning(f'File opened: {str(input_file)}')
            lines = input_file.readlines()[1:]
            logger.warning(f'Read lines: {str(lines)}')
            print(lines)
            product_ids = {
                re.match('[0-9]+', pid.split(',', 1)[0]).group()
                for pid
                # read all input file rows, except the header
                in lines
                if re.match('[0-9]+', pid.split(',', 1)[0])
            }

        print(product_ids)
        return product_ids

    def produce(self):
        try:
            logger.warning('Starting CeneoProducer.produce')
            utctime_started = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            utctime_started_short = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            parameters = self.parameters
            logger.warning('Starting parsing products')
            product_ids = self.parse_product_ids()
            logger.warning(f'Products parsed {str(product_ids)}')
            columns_mapping = parameters.get("columns_mapping")
            for batch_i, product_batch in batches(product_ids, batch_size=1000, sleep_time=1):
                logger.info(f"Processing batch: {batch_i}")

                batch_result = scrape_batch(parameters.get("api_url"),
                                            parameters.get("#api_key"),
                                            product_batch)

                results = [
                    # filter item columns to only relevant ones and add utctime_started
                    {
                        **{columns_mapping[colname]: colval for colname, colval in item.items()
                           if colname in columns_mapping.keys()},
                        **{"TS": utctime_started, "COUNTRY": "PL", "DISTRCHAN": "MA", "SOURCE": "ceneo",
                           "SOURCE_ID": f"ceneo_PL_{utctime_started_short}", "FREQ": "d"}
                    }
                    for item
                    in batch_result
                    # drop products not on ceneo
                    if item.get("product_CeneoProdID")
                       # ceneo records sometimes lack eshop name
                       and item.get("CustName", "") != ""
                       # records without price are useless
                       and item.get("Price", "") != ""
                    # drop empty sublists or None results
                    if batch_result
                ]

                logger.info(f"Batch {batch_i} results collected. Queueing.")

                self.task_queue.put(results)

            logger.info("Iteration over. Putting DONE to queue.")
        except Exception as e:
            trace = 'Traceback:\n' + ''.join(format_tb(e.__traceback__))
            logger.error(f'Error occurred {e}', extra={'full_message': trace})
        finally:
            self.task_queue.put("DONE")
