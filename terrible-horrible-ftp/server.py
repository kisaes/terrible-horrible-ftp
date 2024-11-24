import concurrent.futures
import logging
import os.path
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


# noinspection PyMethodMayBeStatic
class FTPConnection:

    def __init__(self, _socket: socket.socket, address, selector, thread_pool):
        self.socket = _socket
        self.address = address

        self.selector = selector
        self.thread_pool = thread_pool
        self.buffered_reader = _socket.makefile()

        self.current_directory = "/home"
        self._transfer_socket = None

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

    def user_command(self, _):
        return 230, "Login successful"

    def _resolve_path(self, path):
        return os.path.normpath(os.path.join(self.current_directory, path))

    def cwd_command(self, path):
        new_path = self._resolve_path(path)

        if not os.path.exists(new_path):
            return 550, f"{new_path} does not exist"

        self.current_directory = new_path
        return 250, self.current_directory

    # noinspection SpellCheckingInspection
    def cdup_command(self):
        self.current_directory = os.path.dirname(self.current_directory)
        return 200, self.current_directory

    def pwd_command(self):
        return 257, f'"{self.current_directory}"'

    def quit_command(self):
        return 221, "Service closing control connection"

    def rein_command(self):
        self.selector.unregister(self.socket)
        # noinspection PyProtectedMember
        self.selector.register(self.socket, selectors.EVENT_READ,
                               FTPConnection(self.socket, self.address, self.selector, self.thread_pool)._read_command)

    def port_command(self, weird_address):
        self._transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)

        split = weird_address.split(",")
        address = ".".join(split[:4]), (int(split[4]) << 8) + int(split[5])

        self._transfer_socket.connect(address)
        return 200, "Okay"

    def pasv_command(self):
        # noinspection PyAttributeOutsideInit
        self._passive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._passive_socket.listen()

        address = self._passive_socket.getsockname()
        return 227, f"{address[0].replace(".", ",")},{address[1] >> 8},{address[1] & 0xff}"

    @property
    def transfer_socket(self):
        if self._transfer_socket is None:
            self._transfer_socket, _ = self._passive_socket.accept()
        return self._transfer_socket

    # noinspection SpellCheckingInspection
    def syst_command(self):
        return 215, "UNIX"

    def noop_command(self):
        return 200, "Okay"
