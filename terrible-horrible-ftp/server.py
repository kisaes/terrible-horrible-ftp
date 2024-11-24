import concurrent.futures
import logging
import selectors
import socket


class FTPServer:

    def __init__(self, address):
        self.address = address
        self.selector = selectors.DefaultSelector()

        self.log = logging.getLogger(self.__class__.__name__)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor()

    def listen(self):
        # noinspection PyAttributeOutsideInit
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)

        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.address)

        self.log.debug("socket bound to %s:%d", *self.address)

        self.selector.register(self.socket, selectors.EVENT_READ, self.accept)
        self.socket.listen()

        self.log.info("listening!")
        self.run_forever()

    def accept(self):
        client, address = self.socket.accept()
        self.log.debug("new connection from %s:%d", *address)

        # noinspection PyProtectedMember
        self.selector.register(client, selectors.EVENT_READ,
                               FTPConnection(client, address, self.selector, self.thread_pool)._read_command)

    def run_forever(self):
        try:
            while True:
                for key, mask in self.selector.select():
                    key.data()

        except KeyboardInterrupt:
            self.log.info("Interrupted, stopping")

        finally:
            self.socket.close()
            self.selector.close()

            # cancel all pending operations
            self.thread_pool.shutdown(cancel_futures=True)


class FTPConnection:

    def __init__(self, _socket: socket.socket, address, selector, thread_pool):
        self.socket = _socket
        self.address = address

        self.selector = selector
        self.thread_pool = thread_pool
        self.buffered_reader = _socket.makefile()

        self.log = logging.getLogger("{:s}:{:d}".format(*address))
        self.socket.send(b"220 Service ready for new user\r\n")

    def _read_command(self):
        self.log.debug("heads up! network activity")
        line = self.buffered_reader.readline()

        if not line:
            self.log.debug("connection closed by client")
            self.socket.close()

            self.buffered_reader.close()
            self.selector.unregister(self.socket)

        else:
            command, *args = line.split()
            self.log.debug("command=%s, args=%s", command, args)

            code, message = 502, "Not implemented"
            method = getattr(self, f"{command.lower()}_command", None)

            if method is not None:
                code, message = method(*args)

            self.log.debug('code=%d, message="%s"', code, message)
            self.socket.send(f"{code} {message}\r\n".encode())
