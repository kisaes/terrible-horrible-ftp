import concurrent.futures
import datetime
import functools
import hashlib
import logging
import os.path
import pickle
import random
import selectors
import socket
import ssl
import stat
import struct
import sys
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

        os.timerfd_settime(self.republish_timer, initial=self.republish_interval, interval=self.republish_interval)
        self.selector.register(self.republish_timer, selectors.EVENT_READ, self.republish)

        self.selector.register(self.control_socket, selectors.EVENT_READ, self.accept)
        self.selector.register(self.multicast_socket, selectors.EVENT_READ, self.multicast_read)

        key = int.from_bytes(hashlib.sha1('/'.encode()).digest())
        self.storage[key] = Inode(path='/', mode=0o40644, contents={})

        self.control_socket.listen()
        self.run_forever()

    def run_forever(self):
        while True:
            try:  # TODO
                for key, mask in self.selector.select():
                    key.data()
            except KeyboardInterrupt:
                break

    def accept(self):
        client, address = self.control_socket.accept()
        self.logger.debug('new connection from (%s:%d)', *address)
        self.selector.register(client, selectors.EVENT_READ, FTPConnection(client, self).client_read)

    def remote_ping(self):
        int.from_bytes(os.read(self.ping_timer, 8), sys.byteorder)
        message = pickle.dumps({'node_id': self.node_id, 'method': 'ping'})

        try:
            self.logger.debug('sending ping to %s:%d', *self.multicast_address)
            self.multicast_socket.sendto(message, self.multicast_address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def disconnected(self):
        self.selector.unregister(self.ping_timer)
        self.selector.register(self.ping_timer, selectors.EVENT_READ, self.try_reconnect)
        self.last_seen = {}
        self.storage = {}
        self.nodes = {}

    def try_reconnect(self):
        int.from_bytes(os.read(self.ping_timer, 8), sys.byteorder)
        message = pickle.dumps({'node_id': self.node_id, 'method': 'ping'})

        try:
            self.logger.debug('sending ping to %s:%d', *self.multicast_address)
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

    def ping(self, message):
        self.logger.debug('ping from %s:%d', *message['address'])
        self.update_peers(message['node_id'], message['address'])

    def update_peers(self, node_id, address):
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

    def multicast_read(self):
        buffer, address = self.multicast_socket.recvfrom(4096)
        message = pickle.loads(buffer)
        getattr(self, message['method'])({'address': address, **message})

    def remote_find_node(self, address, key, callback, failure):
        op_id = random.getrandbits(160)
        message = pickle.dumps({'method': 'find_node', 'node_id': self.node_id, 'op_id': op_id, 'key': key})

        timer = os.timerfd_create(time.CLOCK_MONOTONIC)
        os.timerfd_settime(timer, initial=self.timeout)

        self.pending[op_id] = {'callback': callback, 'timeout': timer}
        self.selector.register(timer, selectors.EVENT_READ, functools.partial(failure, op_id))

        try:
            self.multicast_socket.sendto(message, address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def find_node(self, message):
        self.update_peers(message['node_id'], message['address'])
        address = message.pop('address')
        message['result'] = self.nearest(message['key'])
        message['method'] = 'find_node_response'
        message['node_id'] = self.node_id

        try:
            self.multicast_socket.sendto(pickle.dumps(message), address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def nearest(self, key):
        buckets = [bucket for bucket in self.nodes.values()]
        neighbors = [(node_id ^ key, node_id, address) for bucket in buckets for node_id, address in bucket.items()]
        return sorted(neighbors)[:3]

    def find_node_response(self, message):
        self.update_peers(message['node_id'], message['address'])

        pending = self.pending.pop(message['op_id'])
        self.selector.unregister(pending['timeout'])

        os.close(pending['timeout'])
        pending['callback'](message)

    def store(self, message):
        self.update_peers(message['node_id'], message['address'])
        self.storage[message['key']] = message['value']

    def remote_store(self, address, key, value):
        message = pickle.dumps({'method': 'store', 'node_id': self.node_id, 'key': key, 'value': value})

        try:
            self.multicast_socket.sendto(message, address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def find_closest(self, key, callback):
        op_id = random.getrandbits(256)
        self.pending[op_id] = {'pending': [], 'contacted': [], 'responding': []}

        for item in self.nearest(key):
            self.pending[op_id]['pending'].append(item)
            self.pending[op_id]['contacted'].append(item)
            self.remote_find_node(item[2], key, functools.partial(self.closest_found, op_id, item, callback),
                                  functools.partial(self.remove_pending, op_id, item, callback))

    def closest_found(self, op_id, item, callback, message):
        self.update_peers(message['node_id'], message['address'])

        for item1 in message['result']:
            if item1[1] != self.node_id and item1 not in self.pending[op_id]['contacted']:
                self.pending[op_id]['pending'].append(item1)
                self.pending[op_id]['contacted'].append(item1)
                self.remote_find_node(item1[2], message['key'],
                                      functools.partial(self.closest_found, op_id, item1, callback),
                                      functools.partial(self.remove_pending, op_id, item1, callback))

        if item not in self.pending[op_id]['responding']:
            self.pending[op_id]['responding'].append(item)

        self.pending[op_id]['pending'].remove(item)

        if not len(self.pending[op_id]['pending']):
            callback(sorted(self.pending[op_id]['responding'])[:3])
            self.pending.pop(op_id)

    def remove_pending(self, op_id, item, callback, rpc_id):
        self.pending[op_id]['pending'].remove(item)
        pending = self.pending.pop(rpc_id)
        self.selector.unregister(pending['timeout'])
        os.close(pending['timeout'])

        bucket = self.nodes.setdefault(item[1].bit_length(), {})
        if item[1] in bucket:
            self.nodes[item[1].bit_length()].pop(item[1])

        if not len(self.pending[op_id]['pending']):
            callback(sorted(self.pending[op_id]['responding'])[:3])
            self.pending.pop(op_id)

    def republish(self):
        int.from_bytes(os.read(self.republish_timer, 8), sys.byteorder)
        for key, value in self.storage.copy().items():
            if key in self.storage:
                self.storage.pop(key)
            if value.alive_counter:
                value.alive_counter -= value.tombstone
                self.find_closest(key, functools.partial(self.replicate, key, value))

    def replicate(self, key, value, nearest):
        for _, node_id, address in nearest:
            self.remote_store(address, key, value)

    def remote_find_value(self, address, key, callback, failure):
        op_id = random.getrandbits(256)
        message = pickle.dumps({'method': 'find_value', 'node_id': self.node_id, 'op_id': op_id, 'key': key})

        timer = os.timerfd_create(time.CLOCK_MONOTONIC)
        os.timerfd_settime(timer, initial=self.timeout)

        self.pending[op_id] = {'callback': callback, 'timeout': timer}
        self.selector.register(timer, selectors.EVENT_READ, functools.partial(failure, op_id))

        try:
            self.multicast_socket.sendto(message, address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def find_value(self, message):
        self.update_peers(message['node_id'], message['address'])
        address = message.pop('address')
        message['result'] = self.storage.get(message.pop('key'))
        message['method'] = 'find_value_response'
        message['node_id'] = self.node_id

        try:
            self.multicast_socket.sendto(pickle.dumps(message), address)
        except OSError as error:
            self.logger.debug('error: %d, %s', error.errno, error.strerror)
            self.disconnected()

    def find_value_response(self, message):
        self.update_peers(message['node_id'], message['address'])
        pending = self.pending.pop(message['op_id'])
        self.selector.unregister(pending['timeout'])
        os.close(pending['timeout'])
        pending['callback'](message)


class FTPConnection:

    def __init__(self, client: socket.socket, server: FTPServer):
        self.client = client
        self.server = server
        self.buffered = client.makefile()

        self.temp_socket = None
        self.working_directory = '/'
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
            self.server.logger.debug('command: %s, args: %s', command, args)

            try:
                getattr(self, f'{command.lower()}_command')(*args)
            except Exception as error:
                self.client.send(f'500 {error}\r\n'.encode())

    def resolve_path(self, path):
        return os.path.normpath(os.path.join(self.working_directory, path))

    def guard(self, method):
        try:
            method()
        except Exception as error:
            self.client.send(f'500 {error}\r\n'.encode())

    def store_inode(self, inode):
        key = int.from_bytes(hashlib.sha1(inode.path.encode()).digest())
        self.server.find_closest(key, functools.partial(self.store_inode_callback, key, inode))

    def store_inode_callback(self, key, inode, nearest):
        for _, node_id, address in nearest:
            self.server.remote_store(address, key, inode)

    def exists(self, path, callback):
        key = int.from_bytes(hashlib.sha1(path.encode()).digest())
        self.server.find_closest(key, functools.partial(self.exists_callback, key, path, callback))

    def exists_callback(self, key, path, callback, nearest):
        self.server.remote_find_value(nearest[0][2], key, functools.partial(callback, key, path),
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
        self.exists(self.resolve_path(path), self.cwd_success)

    def cwd_success(self, key, path, message):
        if not message['result'] or not message['result'].is_directory():
            self.client.send(b'550 Wrong path\r\n')
        else:
            self.working_directory = path
            self.client.send(f'250 "{self.working_directory}"\r\n'.encode())

    def auth_command(self, mechanism):
        if mechanism.lower() not in ['ssl', 'tls']:
            self.client.send(b'500 Not supported\r\n')
        ssl_socket = self.server.context.wrap_socket(self.client, server_side=True)
        self.client.send(b'200 Okay\r\n')
        self.client = ssl_socket

    def port_command(self, weird_address):
        self.temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        split = weird_address.split(",")
        address = ".".join(split[:4]), (int(split[4]) << 8) + int(split[5])
        self.temp_socket.connect(address)
        self.client.send(b'200 Okay\r\n')

    # noinspection SpellCheckingInspection
    def nlst_command(self, *args):
        path = self.resolve_path(args[0]) if len(args) else self.working_directory
        self.exists(path, self.nlst_success)

    # noinspection SpellCheckingInspection
    def nlst_success(self, key, path, message):
        if not message['result'] or not message['result'].is_directory():
            self.client.send(b'550 Wrong path\r\n')
        else:
            contents = message['result'].contents.keys()
            self.client.send(b'125 Transfer in progress\r\n')
            if len(contents):
                self.temp_socket.send(('\r\n'.join(contents) + '\r\n').encode())
            self.temp_socket.close()
            self.client.send(b'226 Requested action complete\r\n')

    def list_command(self, *args):
        path = self.resolve_path(args[0]) if len(args) else self.working_directory
        self.exists(path, self.list_success)

    def list_success(self, key, path, message):
        if not message['result'] or not message['result'].is_directory():
            self.client.send(b'550 Wrong path\r\n')
        else:
            contents = [str(entry) for entry in message['result'].contents.values()]
            self.client.send(b'125 Transfer in progress\r\n')
            if len(contents):
                self.temp_socket.send(('\r\n'.join(contents) + '\r\n').encode())
            self.temp_socket.close()
            self.client.send(b'226 Requested action complete\r\n')

    def mkd_command(self, path):
        self.exists(os.path.dirname(self.resolve_path(path)),
                    functools.partial(self.mkd_success, self.resolve_path(path)))

    # noinspection SpellCheckingInspection
    def mkd_success(self, path, key, dirname, message):
        if not message['result'] or not message['result'].is_directory():
            self.client.send(b'550 Wrong path\r\n')
        else:
            if path in message['result'].contents:
                self.client.send(b'550 Wrong path\r\n')
            else:
                message['result'].contents[path] = Inode(path, mode=0o40644, contents={})
                self.store_inode(message['result'].contents[path])
                self.store_inode(message['result'])
                self.client.send(b'250 Okay\r\n')

    # noinspection SpellCheckingInspection
    def stor_command(self, path):
        self.exists(os.path.dirname(self.resolve_path(path)),
                    functools.partial(self.stor_success, self.resolve_path(path)))

    # noinspection SpellCheckingInspection
    def stor_success(self, path, key, dirname, message):
        if not message['result'] or not message['result'].is_directory():
            self.client.send(b'550 Wrong path\r\n')
        else:
            self.server.thread_pool.submit(self.guard, functools.partial(self.get_file, path, message))
            self.client.send(b'125 Transfer in progress\r\n')

    def get_file(self, path, message):
        buffered = self.temp_socket.makefile('rb')
        contents = buffered.read()

        buffered.close()
        self.temp_socket.close()

        message['result'].contents[path] = Inode(path, 0o100644, contents)
        self.store_inode(message['result'].contents[path])
        self.store_inode(message['result'])
        self.client.send(b'250 Okay\r\n')

    # noinspection SpellCheckingInspection
    def retr_command(self, path):
        self.exists(self.resolve_path(path), self.retr_success)

    # noinspection SpellCheckingInspection
    def retr_success(self, key, path, message):
        if not message['result'] or not message['result'].is_regular_file():
            self.client.send(b'550 Wrong path\r\n')
        else:
            self.server.thread_pool.submit(self.guard, functools.partial(self.send_file, message))
            self.client.send(b'125 Transfer in progress\r\n')

    def send_file(self, message):
        self.temp_socket.sendall(message['result'].contents)
        self.temp_socket.close()
        self.client.send(b'226 Requested action complete\r\n')

    # noinspection SpellCheckingInspection
    def dele_command(self, path):
        self.exists(os.path.dirname(self.resolve_path(path)),
                    functools.partial(self.dele_success, self.resolve_path(path)))

    # noinspection SpellCheckingInspection
    def dele_success(self, path, key, dirname, message):
        if not message['result'] or not message['result'].is_directory():
            self.client.send(b'550 Wrong path\r\n')
        else:
            if path not in message['result'].contents or not message['result'].contents[path].is_regular_file():
                self.client.send(b'550 Wrong path\r\n')
            else:
                file = message['result'].contents.pop(path)
                file.tombstone = True

                self.store_inode(message['result'])
                self.store_inode(file)
                self.client.send(b'250 Okay\r\n')

    # noinspection SpellCheckingInspection
    def rnfr_command(self, path):
        # noinspection PyAttributeOutsideInit
        self.rename_path = self.resolve_path(path)
        self.client.send(b'350 Pending further information\r\n')

    # noinspection SpellCheckingInspection
    def rnto_command(self, path):
        self.exists(os.path.dirname(self.rename_path),
                    functools.partial(self.rnto_success, self.rename_path, self.resolve_path(path)))

    # noinspection SpellCheckingInspection
    def rnto_success(self, old_path, path, key, dirname, message):
        if not message['result'] or not message['result'].is_directory():
            self.client.send(b'550 Wrong path\r\n')
        else:
            if old_path not in message['result'].contents or message['result'].contents[old_path].tombstone:
                self.client.send(b'550 Wrong path\r\n')
            else:
                file = message['result'].contents.pop(old_path)
                file.tombstone = True
                self.store_inode(file)

                message['result'].contents[path] = Inode(path, 0o100644, file.contents)
                self.store_inode(message['result'].contents[path])
                self.store_inode(message['result'])
                self.client.send(b'250 Okay\r\n')

    def rmd_command(self, path):
        self.exists(os.path.dirname(self.resolve_path(path)),
                    functools.partial(self.rmd_success, self.resolve_path(path)))

    # noinspection SpellCheckingInspection
    def rmd_success(self, path, key, dirname, message):
        if not message['result'] or not message['result'].is_directory():
            self.client.send(b'550 Wrong path\r\n')
        else:
            if path not in message['result'].contents or not message['result'].contents[path].is_directory():
                self.client.send(b'550 Wrong path\r\n')
            else:
                if len(message['result'].contents[path].contents):
                    self.client.send(b'550 Directory not empty\r\n')
                else:
                    deleted_file = message['result'].contents.pop(path)
                    deleted_file.tombstone = True

                    self.store_inode(message['result'])
                    self.store_inode(deleted_file)
                    self.client.send(b'250 Okay\r\n')


class Inode:

    # noinspection SpellCheckingInspection
    def __init__(self, path, mode, contents):
        self.path = path
        self.mode = mode
        self.alive_counter = 5
        self.tombstone = False
        self.contents = contents

    def __str__(self):
        return (f'{stat.filemode(self.mode)} '
                f'{1 if stat.S_ISREG(self.mode) else len(self.contents)} '
                'root root '
                f'{4096 if stat.S_ISDIR(self.mode) else len(self.contents)} '
                f'{datetime.datetime.now().strftime('%b %d %H:%M')} '
                f'{os.path.basename(self.path)}')

    def is_directory(self):
        return stat.S_ISDIR(self.mode) and not self.tombstone

    def is_regular_file(self):
        return stat.S_ISREG(self.mode) and not self.tombstone
