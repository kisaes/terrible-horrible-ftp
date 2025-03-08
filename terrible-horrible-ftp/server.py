import concurrent.futures
import datetime
import functools
import hashlib
import logging
import os.path
import pickle
import random
import selectors
import shutil
import socket
import ssl
import stat
import struct
import sys
import tempfile
import time


class FTPServer:

    def __init__(self, multicast_address, server_address, ping_interval=5, republish_interval=5, timeout=1):
        self.logger = logging.getLogger('terrible.horrible.ftp')
        self.selector = selectors.DefaultSelector()

        self.ping_interval = ping_interval
        self.ping_timer = os.timerfd_create(time.CLOCK_MONOTONIC)

        self.multicast_address = multicast_address
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

        self.node_id = random.getrandbits(160)
        self.last_seen = {}
        self.nodes = {}

        self.republish_interval = republish_interval
        self.republish_timer = os.timerfd_create(time.CLOCK_MONOTONIC)

        self.timeout = timeout
        self.pending = {}
        self.storage = {}

        self.server_address = server_address
        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)

        self.spoiling = {}
        self.cache_retention = 10

        self.tempdir = tempfile.TemporaryDirectory()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor()

        self.context = ssl.SSLContext()
        self.context.load_cert_chain(certfile='cert.pem', keyfile='key.pem', password='pass')

    def listen(self):
        self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_socket.bind(('', self.multicast_address[1]))

        self.control_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.control_socket.bind(self.server_address)

        struct_pack = struct.pack('4sI', socket.inet_aton(self.multicast_address[0]), socket.INADDR_ANY)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, struct_pack)

        os.timerfd_settime(self.ping_timer, initial=1, interval=self.ping_interval)
        self.selector.register(self.ping_timer, selectors.EVENT_READ, self.remote_ping)

        os.timerfd_settime(self.republish_timer, initial=1, interval=self.republish_interval)
        self.selector.register(self.republish_timer, selectors.EVENT_READ, self.republish)

        self.selector.register(self.control_socket, selectors.EVENT_READ, self.accept)
        self.selector.register(self.multicast_socket, selectors.EVENT_READ, self.multicast_read)

        hash_path = self.hash_path('/')
        with self.open_key(hash_path, 'wb') as file:
            # noinspection PyTypeChecker
            pickle.dump({}, file)

        self.storage[hash_path] = Inode('/', 0o40644, 4096, 2, datetime.datetime.fromtimestamp(0))
        self.spoiling[hash_path] = self.cache_retention

        self.control_socket.listen()
        self.run_forever()

    def run_forever(self):
        while True:
            try:
                for key, mask in self.selector.select():
                    key.data()
            except KeyboardInterrupt:
                self.logger.info('Interrupted, stopping')
                break

    def accept(self):
        client, address = self.control_socket.accept()
        self.logger.debug('new connection from (%s:%d)', *address)
        self.selector.register(client, selectors.EVENT_READ, FTPConnection(client, self).client_read)

    def multicast_read(self):
        buffer, address = self.multicast_socket.recvfrom(4096)
        message = pickle.loads(buffer)
        getattr(self, message['method'])({'address': address, **message})

    def remote_ping(self):
        int.from_bytes(os.read(self.ping_timer, 8), sys.byteorder)
        message = pickle.dumps({'node_id': self.node_id, 'method': 'ping'})

        try:
            self.logger.debug('ping %s:%d', *self.multicast_address)
            self.multicast_socket.sendto(message, self.multicast_address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def disconnected(self):
        self.selector.unregister(self.ping_timer)
        self.selector.register(self.ping_timer, selectors.EVENT_READ, self.try_reconnect)
        self.last_seen = {}
        self.spoiling = {}
        self.storage = {}
        self.nodes = {}

    def try_reconnect(self):
        int.from_bytes(os.read(self.ping_timer, 8), sys.byteorder)
        message = pickle.dumps({'node_id': self.node_id, 'method': 'ping'})

        try:
            self.logger.debug('ping %s:%d', *self.multicast_address)
            self.multicast_socket.sendto(message, self.multicast_address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
        else:
            self.logger.debug('message went through, reconnecting')
            self.reconnect()

    def reconnect(self):
        self.selector.unregister(self.ping_timer)
        self.selector.register(self.ping_timer, selectors.EVENT_READ, self.remote_ping)
        struct_pack = struct.pack('4sI', socket.inet_aton(self.multicast_address[0]), socket.INADDR_ANY)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, struct_pack)

    def ping(self, remote_message):
        self.logger.debug('ping from %s:%d', *remote_message['address'])
        self.update_peers(remote_message['address'], remote_message['node_id'])

    def update_peers(self, address, node_id):
        bucket = self.nodes.setdefault(node_id.bit_length(), {})

        if node_id in bucket:
            self.nodes[node_id.bit_length()].pop(node_id)

        if len(self.nodes[node_id.bit_length()]) < 3:
            self.last_seen[node_id] = time.monotonic()
            self.nodes[node_id.bit_length()][node_id] = address
        else:
            # Changed in version 3.7: Dictionary order is guaranteed to be insertion order.
            # This behavior was an implementation detail of CPython from 3.6.
            key, value = next(iter(bucket.items()))

            if time.monotonic() - self.last_seen[key] > self.ping_interval:
                self.last_seen.pop(key)
                self.nodes[node_id.bit_length()].pop(key)

                self.last_seen[node_id] = time.monotonic()
                self.nodes[node_id.bit_length()][node_id] = address

    def remote_find_node(self, address, key, callback, failure):
        op_id = random.getrandbits(160)
        message = pickle.dumps({'method': 'find_node', 'node_id': self.node_id, 'op_id': op_id, 'key': key})

        timer = os.timerfd_create(time.CLOCK_MONOTONIC)
        os.timerfd_settime(timer, initial=self.timeout)

        self.pending[op_id] = {'success': callback, 'timeout': timer}
        self.selector.register(timer, selectors.EVENT_READ,
                               functools.partial(self.failure, op_id, failure))

        try:
            self.logger.debug('find_node %s:%d', *address)
            self.multicast_socket.sendto(message, address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def failure(self, op_id, failure):
        pending = self.pending.pop(op_id)
        self.selector.unregister(pending['timeout'])
        os.close(pending['timeout'])
        failure()

    def find_node(self, remote_message):
        self.update_peers(remote_message['address'], remote_message['node_id'])
        message = pickle.dumps({'method': 'response', 'node_id': self.node_id, 'op_id': remote_message['op_id'],
                                'result': self.nearest_nodes(remote_message['key']), 'key': remote_message['key']})

        try:
            self.multicast_socket.sendto(message, remote_message['address'])
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def nearest_nodes(self, key):
        neighbors = [(node_id ^ key, node_id, address) for bucket in self.nodes.values()
                     for node_id, address in bucket.items()]
        return sorted(neighbors)[:3]

    def response(self, remote_message):
        self.update_peers(remote_message['address'], remote_message['node_id'])
        pending = self.pending.pop(remote_message['op_id'])

        self.selector.unregister(pending['timeout'])
        os.close(pending['timeout'])
        pending['success'](remote_message)

    def remote_store(self, address, key, value):
        message = pickle.dumps({'method': 'store', 'node_id': self.node_id, 'key': key, 'value': value})

        try:
            self.multicast_socket.sendto(message, address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def remote_find_value(self, address, key, success, failure):
        op_id = random.getrandbits(160)
        message = pickle.dumps({'method': 'find_value', 'node_id': self.node_id, 'op_id': op_id, 'key': key})

        timer = os.timerfd_create(time.CLOCK_MONOTONIC)
        os.timerfd_settime(timer, initial=self.timeout)

        self.pending[op_id] = {'success': success, 'timeout': timer}
        self.selector.register(timer, selectors.EVENT_READ,
                               functools.partial(self.failure, op_id, failure))

        try:
            self.logger.debug('find_value %s:%d', *address)
            self.multicast_socket.sendto(message, address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def find_value(self, remote_message):
        self.update_peers(remote_message['address'], remote_message['node_id'])
        message = pickle.dumps({'method': 'response', 'node_id': self.node_id, 'op_id': remote_message['op_id'],
                                'result': self.storage.get(remote_message['key'])})

        try:
            self.multicast_socket.sendto(message, remote_message['address'])
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def replicate(self, key, value, nearest):
        for _, node_id, address in nearest:
            self.remote_store(address, key, value)

    def remote_want_file(self, address, key, success, failure):
        op_id = random.getrandbits(160)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)

        server_socket.bind((self.get_address(), 0))
        server_socket.listen()

        message = pickle.dumps(
            {'method': 'want_file', 'node_id': self.node_id, 'op_id': op_id, 'key': key,
             'server_socket': server_socket.getsockname()})

        timer = os.timerfd_create(time.CLOCK_MONOTONIC)
        os.timerfd_settime(timer, initial=self.timeout)

        self.pending[op_id] = {'server_socket': server_socket, 'timeout': timer}
        self.selector.register(timer, selectors.EVENT_READ, functools.partial(self.want_file_failure, op_id, failure))

        self.selector.register(server_socket, selectors.EVENT_READ,
                               functools.partial(self.want_file_success, op_id, success))

        try:
            self.multicast_socket.sendto(message, address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    @staticmethod
    def get_address():
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        client.connect(('1.1.1.1', 1234))
        return client.getsockname()[0]

    def want_file_failure(self, op_id, failure):
        pending = self.pending.pop(op_id)

        self.selector.unregister(pending['timeout'])
        self.selector.unregister(pending['server_socket'])

        pending['server_socket'].close()
        os.close(pending['timeout'])
        failure()

    def want_file_success(self, op_id, success):
        pending = self.pending.pop(op_id)

        self.selector.unregister(pending['timeout'])
        os.close(pending['timeout'])

        self.selector.unregister(pending['server_socket'])
        client, address = pending['server_socket'].accept()
        pending['server_socket'].close()
        success(client)

    def want_file(self, remote_message):
        self.update_peers(remote_message['address'], remote_message['node_id'])
        self.thread_pool.submit(self.send_file, remote_message['server_socket'], remote_message['key'])

    def send_file(self, address, key):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            client.connect(address)
            buffered = client.makefile('wb')

            with self.open_key(key, 'rb') as file:
                # noinspection PyTypeChecker
                shutil.copyfileobj(file, buffered)

            buffered.close()
            client.close()
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)

    def find_closest(self, key, success):
        op_id = random.getrandbits(160)
        self.pending[op_id] = {'pending': [], 'contacted': [], 'responding': []}

        for node in self.nearest_nodes(key):
            self.pending[op_id]['pending'].append(node)
            self.pending[op_id]['contacted'].append(node)
            self.remote_find_node(node[2], key, functools.partial(self.closest_found, op_id, node, success),
                                  functools.partial(self.remove_pending, op_id, node, success))

    def closest_found(self, op_id, node, success, remote_message):
        self.update_peers(remote_message['address'], remote_message['node_id'])

        for node1 in remote_message['result']:
            if node1[1] != self.node_id and node1 not in self.pending[op_id]['contacted']:
                self.pending[op_id]['pending'].append(node1)
                self.pending[op_id]['contacted'].append(node1)
                self.remote_find_node(node1[2], remote_message['key'],
                                      functools.partial(self.closest_found, op_id, node1, success),
                                      functools.partial(self.remove_pending, op_id, node1, success))

        self.pending[op_id]['pending'].remove(node)

        if node not in self.pending[op_id]['responding']:
            self.pending[op_id]['responding'].append(node)

        if not len(self.pending[op_id]['pending']):
            success(sorted(self.pending[op_id]['responding'])[:3])
            self.pending.pop(op_id)

    def remove_pending(self, op_id, node, success):
        self.pending[op_id]['pending'].remove(node)
        bucket = self.nodes.get(node[1].bit_length())

        if bucket is not None and node[1] in bucket:
            self.nodes[node[1].bit_length()].pop(node[1])

        if not len(self.pending[op_id]['pending']):
            success(sorted(self.pending[op_id]['responding'])[:3])
            self.pending.pop(op_id)

    def store(self, remote_message):
        self.update_peers(remote_message['address'], remote_message['node_id'])
        self.spoiling[remote_message['key']] = self.cache_retention

        if remote_message['key'] not in self.storage or remote_message['value'].mtime > self.storage[
            remote_message['key']].mtime:

            if not remote_message['value'].still_alive:
                self.storage[remote_message['key']] = remote_message['value']
            else:
                self.remote_want_file(remote_message['address'], remote_message['key'],
                                      functools.partial(self.sync_file, remote_message['key'], remote_message['value']),
                                      functools.partial(self.maybe_pop_key, remote_message['key']))

    def sync_file(self, key, value, client):
        self.thread_pool.submit(self.sync_read, key, value, client)

    def sync_read(self, key, value, client):
        try:
            size = 0
            with open(os.path.join(self.tempdir.name, str(key) + 'temp'), 'wb') as file:
                while chunk := client.recv(4096):
                    size += file.write(chunk)

            if (value.is_regular_file() and value.size != size) or (value.is_directory() and not size):
                self.logger.debug('transaction failed')
            else:
                with open(os.path.join(self.tempdir.name, str(key) + 'temp'), 'rb') as file:
                    with self.open_key(key, 'wb') as file1:
                        # noinspection PyTypeChecker
                        shutil.copyfileobj(file, file1)
                self.storage[key] = value

        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)

    def maybe_pop_key(self, key):
        if key in self.storage:
            self.storage.pop(key)
            self.spoiling.pop(key)

    def republish(self):
        int.from_bytes(os.read(self.republish_timer, 8), sys.byteorder)
        for key, value in self.storage.copy().items():
            if not (value.dead_counter and key in self.spoiling and self.spoiling[key]):
                self.remove_file(key)
                self.maybe_pop_key(key)
            else:
                self.spoiling[key] -= 1
                value.dead_counter -= not value.still_alive
                self.find_closest(key, functools.partial(self.replicate, key, value))

    def remove_file(self, key):
        try:
            os.unlink(os.path.join(self.tempdir.name, str(key)))
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)

    def open_key(self, key, mode):
        return open(os.path.join(self.tempdir.name, str(key)), mode)

    @staticmethod
    def hash_path(path):
        return int.from_bytes(hashlib.sha1(path.encode()).digest())


class FTPConnection:

    def __init__(self, client: socket.socket, server: FTPServer):
        self.client = client
        self.server = server
        self.buffered = client.makefile()

        self.temp_socket = None
        self.working_directory = '/'

        self.logger = logging.getLogger('terrible.horrible.ftp.connection')
        self.client.send(b'220 Service ready\r\n')

    def client_read(self):
        buffer = self.buffered.readline()
        self.server.logger.debug('%d bytes read', len(buffer))

        if not buffer:
            self.server.selector.unregister(self.client)
            self.buffered.close()
            self.client.close()

        else:
            command, *args = buffer.split()
            self.logger.debug('command: %s, args: %s', command, args)

            try:
                self.exception_handler(getattr(self, f'{command.lower()}_command'), *args)
            except AttributeError as error:
                self.client.send(f'502 {command} not implemented\r\n'.encode())

    def exception_handler(self, block, *args, **kwargs):
        try:
            block(*args, **kwargs)
        except Exception as error:
            # traceback.print_exception(error)
            self.client.send(f'500 {error}\r\n'.encode())

    def resolve_path(self, path):
        return os.path.normpath(os.path.join(self.working_directory, path))

    def store_meta(self, meta, raw_data=None):
        key = self.server.hash_path(meta.path)
        # TODO >>>
        if raw_data is not None:
            with open(os.path.join(self.server.tempdir.name, str(key)), 'wb') as file:
                file.write(raw_data if isinstance(raw_data, bytes) else pickle.dumps(raw_data))
        # TODO <<<
        self.server.find_closest(key, functools.partial(self.store_meta_success, key, meta))

    def store_meta_success(self, key, meta, nearest):
        for _, node_id, address in nearest:
            self.server.remote_store(address, key, meta)

    def find(self, path, success):
        key = self.server.hash_path(path)
        self.server.find_closest(key, functools.partial(self.find_success, path, success))

    def find_success(self, path, success, nearest):
        self.server.remote_find_value(nearest[0][2], self.server.hash_path(path), functools.partial(success, path),
                                      functools.partial(self.client.send, b'550 Wrong path\r\n'))

    def user_command(self, user):
        self.client.send(b'230 Login successful\r\n')

    # noinspection SpellCheckingInspection
    def cdup_command(self):
        self.working_directory = os.path.dirname(self.working_directory)
        self.client.send(f'200 {self.working_directory}\r\n'.encode())

    def pwd_command(self):
        self.client.send(f'257 "{self.working_directory}"\r\n'.encode())

    def quit_command(self):
        self.client.send(b'221 Closing connection\r\n')

    def rein_command(self):
        self.server.selector.unregister(self.client)
        self.server.selector.register(self.client, selectors.EVENT_READ,
                                      FTPConnection(self.client, self.server).client_read)

    # noinspection PyShadowingBuiltins
    def type_command(self, type):
        self.client.send(b'200 Okay\r\n')

    # noinspection SpellCheckingInspection
    def syst_command(self):
        self.client.send(b'215 UNIX\r\n')

    def noop_command(self):
        self.client.send(b'200 Okay\r\n')

    def cwd_command(self, path):
        self.find(self.resolve_path(path), self.cwd_success)

    def cwd_success(self, resolved_path, remote_message):
        if not remote_message['result'] or not remote_message['result'].is_directory():
            self.client.send(b'550 Wrong path\r\n')
        else:
            self.working_directory = resolved_path
            self.client.send(f'250 "{self.working_directory}"\r\n'.encode())

    def auth_command(self, mechanism):
        if mechanism.lower() not in ['ssl', 'tls']:
            self.client.send(b'500 Not supported\r\n')

        self.client.send(b'200 Okay\r\n')
        self.server.selector.unregister(self.client)

        self.client = self.server.context.wrap_socket(self.client, server_side=True)
        self.buffered = self.client.makefile()
        self.server.selector.register(self.client, selectors.EVENT_READ, self.client_read)

    def port_command(self, weird_address):
        self.temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        split = weird_address.split(',')
        self.temp_socket.connect(('.'.join(split[:4]), (int(split[4]) << 8) + int(split[5])))
        self.client.send(b'200 Okay\r\n')

    # noinspection SpellCheckingInspection
    def nlst_command(self, *args):
        path = self.resolve_path(args[0]) if len(args) else self.working_directory
        self.find(path, self.nlst_success)

    # noinspection SpellCheckingInspection
    def nlst_success(self, path, remote_message):
        if not remote_message['result'] or not remote_message['result'].is_directory():
            self.client.send(f'550 Wrong path {path}\r\n'.encode())
        else:
            self.server.remote_want_file(remote_message['address'], self.server.hash_path(path),
                                         functools.partial(self.server.thread_pool.submit,
                                                           functools.partial(self.exception_handler, self.nlst_rw)),
                                         functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    # noinspection SpellCheckingInspection
    def nlst_rw(self, client: socket.socket):
        listing = pickle.load(client.makefile('rb'))
        self.client.send(b'125 Transfer in progress\r\n')
        if len(listing):
            self.temp_socket.send(('\r\n'.join(listing.keys()) + '\r\n').encode())
        self.temp_socket.close()
        self.client.send(b'226 Requested action complete\r\n')

    def list_command(self, *args):
        path = self.resolve_path(args[0]) if len(args) else self.working_directory
        self.find(path, self.list_success)

    def list_success(self, path, remote_message):
        if not remote_message['result'] or not remote_message['result'].is_directory():
            self.client.send(f'550 Wrong path {path}\r\n'.encode())
        else:
            self.server.remote_want_file(remote_message['address'], self.server.hash_path(path),
                                         functools.partial(self.server.thread_pool.submit,
                                                           functools.partial(self.exception_handler, self.list_rw)),
                                         functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    def list_rw(self, client: socket.socket):
        listing = pickle.load(client.makefile('rb'))
        self.client.send(b'125 Transfer in progress\r\n')
        if len(listing):
            self.temp_socket.send(('\r\n'.join([str(entry) for entry in listing.values()]) + '\r\n').encode())
        self.temp_socket.close()
        self.client.send(b'226 Requested action complete\r\n')

    def mkd_command(self, path):
        self.find(os.path.dirname(self.resolve_path(path)),
                  functools.partial(self.mkd_success, self.resolve_path(path)))

    def mkd_success(self, path, parent, remote_message):
        if not remote_message['result'] or not remote_message['result'].is_directory():
            self.client.send(f'550 Wrong path {parent}\r\n'.encode())
        else:
            self.server.remote_want_file(remote_message['address'], self.server.hash_path(parent),
                                         functools.partial(self.server.thread_pool.submit,
                                                           functools.partial(self.exception_handler, self.mkd_read,
                                                                             path, remote_message['result'])),
                                         functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    def mkd_read(self, path, parent, client):
        listing = pickle.load(client.makefile('rb'))

        if os.path.basename(path) in listing:
            self.client.send(f'550 Wrong path {path}\r\n'.encode())
        else:
            listing[os.path.basename(path)] = Inode(path, 0o40644, 4096, 2)
            self.store_meta(listing[os.path.basename(path)], {})

            # noinspection SpellCheckingInspection
            parent.mtime = datetime.datetime.now()
            parent.files += 1

            self.store_meta(parent, pickle.dumps(listing))
            self.client.send(b'250 Okay\r\n')

    def rmd_command(self, path):
        self.find(os.path.dirname(self.resolve_path(path)),
                  functools.partial(self.rmd_success, self.resolve_path(path)))

    def rmd_success(self, path, parent, remote_message):
        if not remote_message['result'] or not remote_message['result'].is_directory():
            self.client.send(f'550 Wrong path {parent}\r\n'.encode())
        else:
            self.server.remote_want_file(remote_message['address'], self.server.hash_path(parent),
                                         functools.partial(self.server.thread_pool.submit,
                                                           functools.partial(self.exception_handler, self.rmd_read,
                                                                             path, remote_message)),
                                         functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    def rmd_read(self, path, remote_message, client):
        listing = pickle.load(client.makefile('rb'))
        if os.path.basename(path) not in listing or not listing[os.path.basename(path)].is_directory():
            self.client.send(f'550 Wrong path {path}\r\n'.encode())
        else:
            self.server.remote_want_file(remote_message['address'], self.server.hash_path(path),
                                         functools.partial(self.server.thread_pool.submit,
                                                           functools.partial(self.exception_handler, self.rmd_read1,
                                                                             path, remote_message['result'], listing)),
                                         functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    def rmd_read1(self, path, parent, parent_listing, client: socket.socket):
        listing = pickle.load(client.makefile('rb'))
        if len(list(listing.keys())):
            self.client.send(f'550 Directory not empty {path}\r\n'.encode())
        else:
            removed = parent_listing.pop(os.path.basename(path))
            # noinspection SpellCheckingInspection
            removed.mtime = datetime.datetime.now()
            removed.still_alive = False

            # noinspection SpellCheckingInspection
            parent.mtime = datetime.datetime.now()
            parent.files -= 1

            self.store_meta(removed)
            self.store_meta(parent, parent_listing)
            self.client.send(b'200 Okay\r\n')

    # noinspection SpellCheckingInspection
    def stor_command(self, path):
        self.find(os.path.dirname(self.resolve_path(path)),
                  functools.partial(self.stor_success, self.resolve_path(path)))

    # noinspection SpellCheckingInspection
    def stor_success(self, path, parent, remote_message):
        if not remote_message['result'] or not remote_message['result'].is_directory():
            self.client.send(f'550 Wrong path {parent}\r\n'.encode())
        else:
            self.server.remote_want_file(remote_message['address'], self.server.hash_path(parent),
                                         functools.partial(self.server.thread_pool.submit,
                                                           functools.partial(self.exception_handler, self.stor_read,
                                                                             path, remote_message['result'])),
                                         functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    # noinspection SpellCheckingInspection
    def stor_read(self, path, parent, client):
        listing = pickle.load(client.makefile('rb'))
        if os.path.basename(path) in listing and listing[os.path.basename(path)].is_directory():
            self.client.send(f'550 Wrong path {path}\r\n'.encode())
        else:
            key = self.server.hash_path(path)
            self.server.thread_pool.submit(self.get_file, path, key, parent, listing)
            self.client.send(b'125 Transfer in progress\r\n')

    def get_file(self, path, key, parent, listing):
        size = 0
        with self.server.open_key(key, 'wb') as file:
            while chunk := self.temp_socket.recv(4096):
                size += file.write(chunk)

        new_file = Inode(path, 0o100644, size)
        # noinspection SpellCheckingInspection
        parent.mtime = datetime.datetime.now()
        parent.files += os.path.basename(path) not in listing
        listing[os.path.basename(path)] = new_file

        self.store_meta(new_file)
        self.store_meta(parent, listing)
        self.client.send(b'250 Okay\r\n')

    # noinspection SpellCheckingInspection
    def retr_command(self, path):
        self.find(self.resolve_path(path), self.retr_success)

    # noinspection SpellCheckingInspection
    def retr_success(self, path, remote_message):
        if not remote_message['result'] or not remote_message['result'].is_regular_file():
            self.client.send(f'550 Wrong path {path}\r\n'.encode())
        else:
            self.server.remote_want_file(remote_message['address'], self.server.hash_path(path),
                                         functools.partial(self.server.thread_pool.submit,
                                                           functools.partial(self.exception_handler, self.send_file)),
                                         functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    def send_file(self, client: socket.socket):
        self.client.send(b'125 Transfer in progress\r\n')
        buffered = self.temp_socket.makefile('wb')
        shutil.copyfileobj(client.makefile('rb'), buffered)
        buffered.close()
        self.temp_socket.close()
        self.client.send(b'226 Requested action complete\r\n')

    # noinspection SpellCheckingInspection
    def dele_command(self, path):
        self.find(os.path.dirname(self.resolve_path(path)),
                  functools.partial(self.dele_success, self.resolve_path(path)))

    # noinspection SpellCheckingInspection
    def dele_success(self, path, parent, remote_message):
        if not remote_message['result'] or not remote_message['result'].is_directory():
            self.client.send(f'550 Wrong path {parent}\r\n'.encode())
        else:
            self.server.remote_want_file(remote_message['address'], self.server.hash_path(parent),
                                         functools.partial(self.server.thread_pool.submit,
                                                           functools.partial(self.exception_handler, self.dele_read,
                                                                             path, remote_message['result'])),
                                         functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    # noinspection SpellCheckingInspection
    def dele_read(self, path, parent, client):
        listing = pickle.load(client.makefile('rb'))
        if not (os.path.basename(path) in listing and listing[os.path.basename(path)].is_regular_file()):
            self.client.send(f'550 Wrong path {path}\r\n'.encode())
        else:
            removed = listing.pop(os.path.basename(path))
            removed.mtime = datetime.datetime.now()
            removed.still_alive = False

            # noinspection SpellCheckingInspection
            parent.mtime = datetime.datetime.now()
            parent.files -= 1

            self.store_meta(removed)
            self.store_meta(parent, listing)
            self.client.send(b'200 Okay\r\n')

    # noinspection SpellCheckingInspection
    def rnfr_command(self, path):
        # noinspection PyAttributeOutsideInit
        self.rename_path = self.resolve_path(path)
        self.client.send(b'350 Pending further information\r\n')

    # noinspection SpellCheckingInspection
    def rnto_command(self, path):
        self.find(os.path.dirname(self.resolve_path(path)),
                  functools.partial(self.rnto_success, self.resolve_path(path)))

    # noinspection SpellCheckingInspection
    def rnto_success(self, path, parent, remote_message):
        if not remote_message['result'] or not remote_message['result'].is_directory():
            self.client.send(f'550 Wrong path {parent} \r\n'.encode())
        else:
            self.server.remote_want_file(remote_message['address'], self.server.hash_path(parent),
                                         functools.partial(self.server.thread_pool.submit,
                                                           functools.partial(self.exception_handler, self.rnto_read,
                                                                             path, remote_message['result'])),
                                         functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    # noinspection SpellCheckingInspection
    def rnto_read(self, path, parent, client):
        listing = pickle.load(client.makefile('rb'))
        if os.path.basename(self.rename_path) not in listing or listing[
            os.path.basename(self.rename_path)].is_directory():
            self.client.send(f'550 Wrong path {self.rename_path}\r\n'.encode())
        else:
            self.find(self.rename_path, functools.partial(self.rename_success, path, parent, listing))

    def rename_success(self, new_path, parent, listing, path, remote_message):
        self.server.remote_want_file(remote_message['address'], self.server.hash_path(path),
                                     functools.partial(self.server.thread_pool.submit,
                                                       functools.partial(self.exception_handler, self.rename_read,
                                                                         new_path, parent, listing)),
                                     functools.partial(self.client.send, b'550 Something went wrong\r\n'))

    def rename_read(self, new_path, parent, listing, client: socket.socket):
        with self.server.open_key(self.server.hash_path(new_path), 'wb') as file:
            # noinspection PyTypeChecker
            shutil.copyfileobj(client.makefile('rb'), file)

        removed = listing.pop(os.path.basename(self.rename_path))
        # noinspection SpellCheckingInspection
        removed.mtime = datetime.datetime.now()
        removed.still_alive = False

        # noinspection SpellCheckingInspection
        parent.mtime = datetime.datetime.now()
        listing[os.path.basename(new_path)] = Inode(new_path, removed.mode, removed.size)

        self.store_meta(removed)
        self.store_meta(parent, listing)
        self.store_meta(listing[os.path.basename(new_path)])
        self.client.send(b'200 Okay\r\n')


class Inode:

    # noinspection SpellCheckingInspection
    def __init__(self, path, mode, size, files=1, mtime=datetime.datetime.now()):
        self.path = path
        self.mode = mode
        self.size = size
        self.files = files
        self.mtime = mtime
        self.dead_counter = 10
        self.still_alive = True

    def __str__(self):
        return (f'{stat.filemode(self.mode)} {self.files} root root {self.size} '
                f'{self.mtime.strftime('%b %d %H:%M')} {os.path.basename(self.path)}')

    def is_directory(self):
        return self.still_alive and stat.S_ISDIR(self.mode)

    def is_regular_file(self):
        return self.still_alive and stat.S_ISREG(self.mode)
