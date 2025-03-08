import argparse
import logging

from server import FTPServer

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    parser.add_argument("--log-level", choices=levels)

    options = parser.parse_args()
    logging.basicConfig(level=options.log_level, format='%(asctime)s %(name)s %(levelname)s %(message)s')

    FTPServer(('224.0.0.1', 5000), ('0.0.0.0', 21)).listen()
