#coding:utf-8

import os, sys
#import traceback
import json


### config

__curdir = os.path.dirname(os.path.abspath(__file__))

__rpc_config_file = os.path.join(__curdir, 'xtdata.ini')

from .IPythonApiClient import rpc_init
__rpc_init_status = rpc_init(__rpc_config_file)

if __rpc_init_status < 0:
    raise Exception(f'rpc init failed, error_code:{__rpc_init_status}, config:{__rpc_config_file}')


### function

def try_create_client():
    from .IPythonApiClient import IPythonApiClient as RPCClient

    cl = RPCClient()
    cl.init()

    ec = cl.load_config(__rpc_config_file, 'client_xtdata')
    if ec < 0:
        raise f'load config failed, file:{__rpc_config_file}'
    return cl


def try_create_connection(addr):
    '''
    addr: 'localhost:58610'
    '''
    ip, port = addr.split(':')
    if not ip:
        ip = 'localhost'
    if not port:
        raise Exception('invalid port')

    cl = try_create_client()
    cl.set_config_addr(addr)

    ec, msg = cl.connect()
    if ec < 0:
        raise Exception((ec, msg))
    return cl


def create_connection(addr):
    try:
        return try_create_connection(addr)
    except Exception as e:
        return None


def scan_available_server():
    try:
        result = []

        config_dir = os.path.abspath(os.path.join(os.environ['USERPROFILE'], '.xtquant'))

        for f in os.scandir(config_dir):
            full_path = f.path

            is_running = False
            try:
                os.remove(os.path.join(full_path, 'running_status'))
            except PermissionError:
                is_running = True
            except Exception as e:
                pass

            if not is_running:
                continue

            try:
                config = json.load(open(os.path.join(full_path, 'xtdata.cfg'), 'r', encoding = 'utf-8'))

                ip = config.get('ip', 'localhost')
                port = config.get('port', None)
                if not port:
                    raise Exception(f'invalid port: {port}')

                addr = f'{ip}:{port}'
                result.append(addr)
            except Exception as e:
                continue

        result.sort()
        return result

    except Exception as e:
        return []


def connect_any(addr_list):
    '''
    addr_list: [ addr, ... ]
        addr: 'localhost:58610'
    '''
    for addr in addr_list:
        try:
            cl = create_connection(addr)
            if cl:
                return cl
        except Exception as e:
            continue

    return None




