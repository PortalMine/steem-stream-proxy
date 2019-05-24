import threading
import logging
import socket
import pickle


class StreamProxyClient:
    def __init__(self, name: str, mode: str, server_address: tuple, subs: list = None, log_level: int = None, log_level_listen: int = None):
        if mode not in ['head', 'irreversible']:
            raise ValueError('mode must be either \'head\' or \'irreversible\'')

        self.log = logging.getLogger('SteamProxyClient-{}'.format(name))
        if log_level:
            if log_level == 'ALL':
                log_level = 0
            elif log_level == 'NORMAL':
                log_level = 15
            self.log.setLevel(log_level)
        else:
            self.log.setLevel('INFO')
        self.log.info('Client created')

        self.log_level_listen = log_level_listen

        self.myself_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.myself_recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.myself_recv.settimeout(30)  # set timeout for receiving messages from server (used to determine whether the server is still online)

        self.address_server = server_address
        self.name = name
        self.mode = mode
        self.subs = subs
        self.running = False

        self.callable_everything = None  # fires every income (needs argument for incoming data)
        self.callable_chain_data = None  # what to do with TXs (needs argument for incoming data)
        self.callable_client_info = None  # what to do with client_info (needs argument for incoming data)
        self.callable_error = None  # what to do with errors (needs argument for incoming data)
        self.callable_client_delete = None  # what to do as client has been deleted
        self.callable_server_stopped = None  # what to do as server has stopped
        self.callable_pong = None  # what to do with pongs

    def set_subscriptions(self, subs: list = None):
        if subs:
            self.subs = subs
        self.log.info('Setting subscriptions on server side: {!s}'.format(self.subs))
        self.myself_send.sendto(pickle.dumps({'command': 'set_subs', 'name': self.name, 'subs': subs}), self.address_server)

    def add_subscriptions(self, subs: list):
        self.myself_send.sendto(pickle.dumps({'command': 'add_subs', 'name': self.name, 'subs': subs}), self.address_server)
        [self.subs.append(x) for x in subs if x not in self.subs]
        self.log.info('Adding subscriptions on server side to: {!s}'.format(self.subs))

    def rem_subscriptions(self, subs: list):
        self.myself_send.sendto(pickle.dumps({'command': 'rem_subs', 'name': self.name, 'subs': subs}), self.address_server)
        [self.subs.remove(x) for x in subs if x in self.subs]
        self.log.info('Removing subscriptions on server side to: {!s}'.format(self.subs))

    def get_info(self):
        if self.running:
            self.myself_send.sendto(pickle.dumps({'command': 'info', 'name': self.name}), self.address_server)
        else:
            self.log.info('Could not ask for client info since not connected to server.')

    def refresh(self):
        self.myself_send.sendto(pickle.dumps({'command': 'refresh', 'name': self.name}), self.address_server)
        self.log.info('Refreshed connection.')

    def ping(self):
        self.myself_send.sendto(pickle.dumps({'command': 'ping', 'name': self.name}), self.address_server)
        self.log.info('Sending ping.')
        if not self.running:
            try:
                while True:
                    data = pickle.loads(self.myself_send.recvfrom(64)[0])
                    if data.get('info') == 'ping_answer':
                        if self.callable_everything:
                            self.callable_everything(data)
                        if self.callable_pong:
                            self.callable_pong()
                        self.log.info('Received pong.')
                        break
            except ConnectionResetError:
                self.log.info('Connection refused on pinged port.')
            except socket.timeout:
                self.log.info('Connection timed out on pinged port.')

    def stop(self):
        self.myself_send.sendto(pickle.dumps({'command': 'stop'}), self.address_server)
        self.log.info('Sending stop signal to server.')

    def start_listen(self, subs: list = None):
        if self.running or [True for x in threading.enumerate() if x.name == 'listen_thread']:
            RuntimeError('Already listening or thread has not ended yet.')
        if subs:
            self.subs = subs
        self.running = True
        threading.Thread(target=self._listen_thread, name='listen_thread').start()
        self.log.info('Starting listening with subs: {}.'.format(self.subs))

    def stop_listen(self):
        if not self.running:
            self.log.info('Could not stop listening since not listening yet.')
        else:
            self.log.info('Stopping listening.')
            self.running = False
            [x.join() for x in threading.enumerate() if x.name == 'listen_thread']
            self.log.info('Stopped listening.')

    def _listen_thread(self):
        self.thread_log = logging.getLogger('SteamProxyClient-{}-listening_thread'.format(self.name))
        if self.log_level_listen:
            if self.log_level_listen == 'ALL':
                self.log_level_listen = 0
            elif self.log_level_listen == 'NORMAL':
                self.log_level_listen = 15
            self.thread_log.setLevel(self.log_level_listen)
        else:
            self.thread_log.setLevel('INFO')
        self.thread_log.info('Listening thread created.')

        if self.subs:
            self.thread_log.info('Subscribing mode "{}" with subs {!s}.'.format(self.mode, self.subs))
            self.myself_recv.sendto(pickle.dumps(
                [{'command': 'register', 'mode': self.mode, 'name': self.name},
                 {'command': 'set_subs', 'subs': self.subs, 'name': self.name}]
            ), self.address_server)
        else:
            self.thread_log.info('Subscribing mode "{}" without subs.'.format(self.mode))
            self.myself_recv.sendto(pickle.dumps({'command': 'register', 'mode': self.mode, 'name': self.name}), self.address_server)

        ping_requested = False
        ping_answered = False
        info_requested = False
        info_answered = False

        while self.running:
            try:
                data, address = self.myself_recv.recvfrom(65536)
                data = pickle.loads(data)
                self.thread_log.log(5, data)
                if data.get('info') and data.get('name') == self.name:
                    self.thread_log.debug(data)
                    if self.callable_everything:
                        self.callable_everything(data)

                    if data['info'] == 'stream_data' and isinstance(data.get('data'), dict):  # got block chain data
                        if self.callable_chain_data:
                            self.callable_chain_data(data.get('data'))
                        self.thread_log.log(5, 'Received stream data: {}'.format(data.get('data')))
                    elif data['info'] == 'client_info' and isinstance(data.get('data'), list):  # got requested client info
                        if self.callable_client_info:
                            self.callable_client_info(data.get('data'))
                        self.thread_log.info('Received client info data: {}'.format(data.get('data')))
                        if info_requested:
                            info_answered = True
                        else:
                            info_answered = False
                    elif data['info'] == 'error' and isinstance(data.get('data'), str):  # error in server
                        if self.callable_error:
                            self.callable_error(data.get('data'))
                        self.thread_log.error('Received error message: {}.'.format(data.get('data')))
                    elif data['info'] == 'refresh_req':
                        self.myself_send.sendto(pickle.dumps([{'command': 'refresh', 'name': self.name}]), self.address_server)
                        self.thread_log.debug('Refreshed subscription.')
                    elif data['info'] == 'client_delete':
                        if self.callable_client_delete:
                            self.callable_client_delete()
                            self.thread_log.info('Client was deleted from server.')
                        self.running = False
                    elif data['info'] == 'stop':
                        if self.callable_server_stopped:
                            self.callable_server_stopped()
                        self.thread_log.info('Server shut down.')
                        self.running = False
                    elif data['info'] == 'ping_answer':
                        if self.callable_pong:
                            self.callable_pong()
                        self.thread_log.info('Received pong.')
                        if ping_requested:
                            ping_answered = True
                        else:
                            ping_answered = False

            except ConnectionResetError:
                self.thread_log.error('connection refused. Server offline.')
                return 2
            except socket.timeout:
                if not ping_requested and not ping_answered and not info_requested and not info_answered:  # no ping test made, no registration test made
                    self.myself_send.sendto(pickle.dumps({'command': 'ping', 'name': self.name}), self.address_server)
                    ping_requested = True

                elif ping_requested and not ping_answered and not info_requested and not info_answered:  # ping test failed, no registration test made
                    self.thread_log.error('ping test failed. Server offline.')
                    return 2

                elif ping_requested and ping_answered and not info_requested and not info_answered:  # ping test successful, no registration test made
                    self.myself_send.sendto(pickle.dumps({'command': 'info', 'name': self.name}), self.address_server)
                    info_requested = True

                elif ping_requested and ping_answered and info_requested and not info_answered:  # ping test successful, registration test failed
                    self.thread_log.error('server online. registration test failed. Not registered.')
                    self.myself_send.sendto(pickle.dumps(  # re-register
                        [{'command': 'register', 'mode': self.mode, 'name': self.name},
                         {'command': 'set_subs', 'subs': self.subs, 'name': self.name}]
                    ), self.address_server)
                    ping_requested = False
                    ping_answered = False
                    info_requested = False
                    info_answered = False

                elif ping_requested and ping_answered and info_requested and info_answered:  # ping test successful, registration test successful
                    self.thread_log.info('all fine! Just rare transaction types...')
                    ping_requested = False
                    ping_answered = False
                    info_requested = False
                    info_answered = False

        self.myself_send.sendto(pickle.dumps({'command': 'unregister', 'name': self.name}), self.address_server)