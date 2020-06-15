import csv
import logging


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

                results_writer.writerows(chunk)
