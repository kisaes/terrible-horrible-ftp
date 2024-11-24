import argparse
import logging

from server import FTPServer

if __name__ == '__main__':
    parser = argparse.ArgumentParser(__name__)

    levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    parser.add_argument("--log-level", default="INFO", choices=levels)

    parser.add_argument("--host", default="")
    parser.add_argument("--port", "-p", type=int, default=21)

    options = parser.parse_args()

    # noinspection SpellCheckingInspection
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M', filename='/tmp/log.txt', filemode='w')

    term = logging.StreamHandler()
    term.setLevel(options.log_level)

    # noinspection SpellCheckingInspection
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    term.setFormatter(formatter)

    logging.getLogger('').addHandler(term)

    FTPServer((options.host, options.port)).listen()
