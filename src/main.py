import concurrent.futures
import csv
import logging
import os
import queue
from collections.abc import Mapping, Iterable

import logging_gelf.formatters
import logging_gelf.handlers
from keboola import docker


class Writer(object):
    def __init__(self, task_queue, columns_list, file_path):
        self.task_queue = task_queue
        self.columns_list = columns_list
        self.file_path = file_path
        self.logger = logging.getLogger('writer')

    def __enter__(self):
        self._file = open(self.file_path, 'w+')
        results_writer = csv.DictWriter(self._file, fieldnames=self.columns_list, extrasaction='ignore')
        results_writer.writeheader()
        self._writer = results_writer
        return self._writer

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.close()

    def __call__(self, *args, **kwargs):
        self.write_loop()

    def write_loop(self):
        with self as results_writer:
            while chunk := self.task_queue.get():
                if chunk == 'DONE':
                    self.logger.info('DONE received. Exiting.')
                    break
                elif isinstance(chunk, Mapping):  # dict, write single row
                    results_writer.writerow(chunk)
                elif isinstance(chunk, Iterable):  # list of dicts, write multiple rows
                    results_writer.writerows(chunk)
                else:
                    logging.error(f'Chunk is neither Mapping, nor Iterable type. Chunk: {chunk}')
                    logging.info('Skipping')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, handlers=[])  # do not create default stdout handler
    logger = logging.getLogger()
    try:
        logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
            host=os.getenv('KBC_LOGGER_ADDR'),
            port=int(os.getenv('KBC_LOGGER_PORT')))
    except TypeError:
        logging_gelf_handler = logging.StreamHandler()

    logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)
    logger.setLevel(logging.INFO)

    colnames = ['AVAILABILITY',
                'COUNTRY',
                'CSE_ID',
                'CSE_URL',
                'DISTRCHAN',
                'ESHOP',
                'FREQ',
                'HIGHLIGHTED_POSITION',
                'MATERIAL',
                'POSITION',
                'PRICE',
                'RATING',
                'REVIEW_COUNT',
                'SOURCE',
                'SOURCE_ID',
                'STOCK',
                'TOP',
                'TS',
                'URL']
    datadir = os.getenv('KBC_DATADIR', '/data/')
    path = f'{os.getenv("KBC_DATADIR")}out/tables/results.csv'
    pipeline = queue.Queue(maxsize=1000)
    conf = docker.Config(datadir)
    params = conf.get_parameters()
    extractor_id = params.pop('extractor_id', None)

    if extractor_id == 'arukereso':
        from extractors.arukereso import ArukeresoProducer as Producer
    elif extractor_id == 'dataweps':
        from extractors.dataweps import DatawepsProducer as Producer
    elif extractor_id == 'ceneo':
        from extractors.ceneo import CeneoProducer as Producer
    elif extractor_id == 'esolutions':
        from extractors.esolutions import ESolutionsProducer as Producer
    elif extractor_id == 'heureka':
        from extractors.heureka import HeurekaProducer as Producer
    elif extractor_id == 'zbozi':
        from extractors.zbozi import ZboziProducer as Producer
    else:
        raise Exception(f'Unrecognized extractor_id "{extractor_id}"')

    # task_queue, datadir, parameters
    producer = Producer(pipeline, datadir, params)
    writer = Writer(pipeline, colnames, path)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(producer.produce)
        executor.submit(writer)
