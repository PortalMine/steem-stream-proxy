from beem.steem import Steem
from beem.blockchain import Blockchain
from beem.instance import set_shared_steem_instance

from copy import copy
import configparser
import threading
from logging.handlers import TimedRotatingFileHandler
import logging
import socket
import pickle
import os

config = configparser.ConfigParser()
config.read('server_config.ini')
set_shared_steem_instance(Steem(node=config.get('STEEM_SETTINGS', 'node', fallback='https://anyx.io')))
standard_ttl = config.get('PROXY_SETTINGS', 'ttl', fallback=20)  # 20 for default, 5 for tests
ttl_tolerance = config.get('PROXY_SETTINGS', 'ttl_tolerance', fallback=2)  # 5 for default, 2 for tests

handlers = []
if config.getboolean('LOGGING', 'log_to_file', fallback=False):
    handlers.append(TimedRotatingFileHandler(filename=config['GENERAL']['log_file'].replace('PID', str(os.getpid())),
                                             when='D',
                                             interval=1,
                                             backupCount=7))
if config.getboolean('LOGGING', 'log_to_console', fallback=True):
    handlers.append(logging.StreamHandler())
logging.basicConfig(level=config.get('LOGGING', 'logging_level_main', fallback='INFO').upper(),
                    format='%(asctime)s:%(levelname)s:%(name)s: %(message)s',
                    handlers=handlers)
del handlers

log_main = logging.getLogger('StreamProxy_main')
log_head = logging.getLogger('StreamProxy_head')
log_irre = logging.getLogger('StreamProxy_irre')
log_level = config.get('LOGGING', 'log_level', fallback='INFO').upper()
if log_level == 'ALL':
    log_level = 0
elif log_level == 'NORMAL':
    log_level = 15
log_main.setLevel(log_level)
log_head.setLevel(log_level)
log_irre.setLevel(log_level)

client_modes = {}  # name: mode
clients_head = {}  # name: [ 0: client_address  1: subscription (transfer, comment, ...)  2: ttl in tx]
clients_irreversible = {}  # name: [ 0: client_address  1: subscription (transfer, comment, ...)  2: ttl in tx]


def stream_head():
    last_block = 0
    log_head.info('starting thread "head"')
    for tx in Blockchain(mode='head').stream():
        log_head.log(5, tx)
        if not running:
            return

        if last_block != tx['block_num']:
            last_block = tx['block_num']
            delete_list = []
            for client_name in clients_head.keys():
                clients_head[client_name][2] -= 1
                if clients_head[client_name][2] <= -ttl_tolerance:
                    delete_list.append(client_name)
                if clients_head[client_name][2] <= 0:
                    myself.sendto(pickle.dumps({'info': 'refresh_req', 'name': client_name}), clients_head[client_name][0])
            for client_name in delete_list:
                myself.sendto(pickle.dumps({'info': 'client_delete', 'name': client_name}), clients_head[client_name][0])
                del clients_head[client_name]
                del client_modes[client_name]

            if not len(clients_head):
                log_head.info('stopping thread "head"')
                return

        for client_name in copy(clients_head).keys():
            if tx.get('type') in clients_head[client_name][1]:
                log_head.log(15, 'Sending {} to "{}" ({!s})'.format(tx.get('type'), client_name, clients_head[client_name][2]))
                myself.sendto(pickle.dumps({'data': tx, 'name': client_name, 'info': 'stream_data'}), clients_head[client_name][0])


def stream_irreversible():
    last_block = 0
    log_irre.info('starting thread "irreversible"')
    for tx in Blockchain(mode='irreversible').stream():
        log_irre.log(5, tx)
        if not running:
            return

        if last_block != tx['block_num']:
            last_block = tx['block_num']
            delete_list = []
            for client_name in clients_irreversible.keys():
                clients_irreversible[client_name][2] -= 1
                if clients_irreversible[client_name][2] <= -ttl_tolerance:
                    delete_list.append(client_name)
                if clients_irreversible[client_name][2] <= 0:
                    myself.sendto(pickle.dumps({'info': 'refresh_req', 'name': client_name}), clients_irreversible[client_name][0])
            for client_name in delete_list:
                myself.sendto(pickle.dumps({'info': 'client_delete', 'name': client_name}), clients_irreversible[client_name][0])
                del clients_irreversible[client_name]
                del client_modes[client_name]
            if not len(clients_irreversible):
                log_irre.info('stopping thread "irreversible"')
                return

        for client_name in copy(clients_irreversible).keys():
            if tx.get('type') in clients_irreversible[client_name][1]:
                log_irre.log(15, 'Sending {} to "{}" ({!s})'.format(tx.get('type'), client_name, clients_irreversible[client_name][2]))
                myself.sendto(pickle.dumps({'data': tx, 'name': client_name, 'info': 'stream_data'}), clients_irreversible[client_name][0])


def execute_cmd(data, address):
    log_main.debug(data)
    if data.get('command'):
        if data['command'] == 'register' and data.get('name') and data.get('mode') in ['head', 'irreversible']:
            if data['name'] in client_modes:
                myself.sendto(pickle.dumps({'info': 'error', 'data': 'name already used'}), address)
                log_main.info('Registration failed since name is already in use. ({})'.format(data['name']))
                return
            if data['mode'] == 'head' and config.getboolean('PROXY_SETTINGS', 'enable_head', fallback=True):
                clients_head[data['name']] = [address, [], standard_ttl]
                log_main.info('Registration to head mode with name "{}" successful.'.format(data['name']))
                if not [True for x in threading.enumerate() if x.name == 'head_thread']:
                    threading.Thread(target=stream_head, name='head_thread').start()
            elif data['mode'] == 'irreversible' and config.getboolean('PROXY_SETTINGS', 'enable_irreversible', fallback=True):
                clients_irreversible[data['name']] = [address, [], standard_ttl]
                log_main.info('Registration to irreversible mode with name {} successful.'.format(data['name']))
                if not [True for x in threading.enumerate() if x.name == 'irreversible_thread']:
                    threading.Thread(target=stream_irreversible, name='irreversible_thread').start()
            else:
                myself.sendto(pickle.dumps({'info': 'error', 'data': 'mode not provided on the server'}), address)
                log_main.info('Registration failed since mode "{}" is not provided on the server.'.format(data['mode']))
                return
            client_modes[data['name']] = data['mode']

        elif data['command'] == 'unregister' and data.get('name') in client_modes:
            if client_modes[data['name']] == 'head':
                myself.sendto(pickle.dumps({'info': 'client_delete', 'name': data['name']}), clients_head[data['name']][0])
                del clients_head[data['name']]
            elif client_modes[data['name']] == 'irreversible':
                myself.sendto(pickle.dumps({'info': 'client_delete', 'name': data['name']}), clients_irreversible[data['name']][0])
                del clients_irreversible[data['name']]
            del client_modes[data['name']]
            log_main.info('Deleted client "{}" from registration.'.format(data['name']))

        elif data['command'] == 'refresh' and data.get('name') in client_modes:
            if client_modes[data['name']] == 'head':
                clients_head[data['name']][2] = standard_ttl
            elif client_modes[data['name']] == 'irreversible':
                clients_irreversible[data['name']][2] = standard_ttl
            log_main.debug('Refreshed connection with client "{}".'.format(data['name']))

        elif data['command'] == 'set_subs' and data.get('name') in client_modes and data.get('subs'):
            if client_modes[data['name']] == 'head':
                clients_head[data['name']][1] = data['subs']
            elif client_modes[data['name']] == 'irreversible':
                clients_irreversible[data['name']][1] = data['subs']
            log_main.info('Set subs of client "{}" to {!s}.'.format(data['name'], data['subs']))

        elif data['command'] == 'add_subs' and data.get('name') in client_modes and data.get('subs'):
            if client_modes[data['name']] == 'head':
                [clients_head[data['name']][1].append(x) for x in data['subs'] if x not in clients_head[data['name']][1]]
                log_main.info('Added subs of client "{}" -> {!s}.'.format(data['name'], clients_head[data['name']][1]))
            elif client_modes[data['name']] == 'irreversible':
                [clients_irreversible[data['name']][1].append(x) for x in data['subs'] if x not in clients_irreversible[data['name']][1]]
                log_main.info('Added subs of client "{}" -> {!s}.'.format(data['name'], clients_irreversible[data['name']][1]))

        elif data['command'] == 'rem_subs' and data.get('name') in client_modes and data.get('subs'):
            if client_modes[data['name']] == 'head':
                [clients_head[data['name']][1].remove(x) for x in data['subs'] if x in clients_head[data['name']][1]]
                log_main.info('Removed subs of client "{}" -> {!s}.'.format(data['name'], clients_head[data['name']][1]))
            elif client_modes[data['name']] == 'irreversible':
                [clients_irreversible[data['name']][1].remove(x) for x in data['subs'] if x in clients_irreversible[data['name']][1]]
                log_main.info('Removed subs of client "{}" -> {!s}.'.format(data['name'], clients_irreversible[data['name']][1]))

        elif data['command'] == 'info' and data.get('name') in client_modes:
            if client_modes[data['name']] == 'head':
                myself.sendto(pickle.dumps({'name': data['name'], 'info': 'client_info', 'data': clients_head[data['name']]}), clients_head[data['name']][0])
            else:
                myself.sendto(pickle.dumps({'name': data['name'], 'info': 'client_info', 'data': clients_irreversible[data['name']]}), clients_irreversible[data['name']][0])
            log_main.info('Sent info of client "{}".'.format(data['name']))

        elif data['command'] == 'stop':
            [myself.sendto(pickle.dumps({'info': 'stop', 'name': client}), clients_head[client][0]) for client in clients_head]
            [myself.sendto(pickle.dumps({'info': 'stop', 'name': client}), clients_irreversible[client][0]) for client in clients_irreversible]
            global running
            running = False

        elif data['command'] == 'ping':
            if data.get('name') in client_modes:
                if client_modes[data['name']] == 'head':
                    myself.sendto(pickle.dumps({'info': 'ping_answer', 'name': data['name']}), clients_head[data['name']][0])
                else:
                    myself.sendto(pickle.dumps({'info': 'ping_answer', 'name': data['name']}), clients_irreversible[data['name']][0])
                log_main.info('Sent pong to client "{}".'.format(data['name']))
            else:
                myself.sendto(pickle.dumps({'info': 'ping_answer'}), address)
                log_main.info('Sent pong to unknown client.')

        elif data['command'] == 'is_registered':
            if data.get('name') in client_modes:
                myself.sendto(pickle.dumps({'info': 'registered', 'data': True}), address)
            else:
                myself.sendto(pickle.dumps({'info': 'registered', 'data': False}), address)
            log_main.info('Sent registration answer to unknown client.')

        else:
            log_main.error('unknown command ({})'.format(data['command']))
    else:
        log_main.error('need command')


myself = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.bind(('localhost', config.get('PROXY_SETTINGS', 'port', fallback=8080)))

running = True
while running:
    log_main.log(5, 'client_modes: ' + str(client_modes))
    log_main.log(5, 'clients_head: ' + str(clients_head))
    log_main.log(5, 'clients_irre: ' + str(clients_irreversible))
    log_main.log(5, 'enumerate: ' + str(threading.enumerate()))

    data_list, address = server_socket.recvfrom(512)
    data_list = pickle.loads(data_list)
    log_main.log(5, data_list)
    if isinstance(data_list, list):
        for data in data_list:
            if isinstance(data, dict):
                execute_cmd(data, address)
    elif isinstance(data_list, dict):
        execute_cmd(data_list, address)

[x.join() for x in threading.enumerate() if x is not threading.current_thread()]

log_main.info('server shut down')
