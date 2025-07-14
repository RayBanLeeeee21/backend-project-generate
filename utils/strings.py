# -*- codding: utf-8 -*-
import json
import os


def read_as_json(file_name='data.json', encoding='utf-8'):
    with open(file_name, encoding=encoding) as fd:
        return json.loads(fd.read())

def write_json(file_name, obj, indent=None):
    directory_path = os.path.dirname(file_name)
    if directory_path and not os.path.exists(directory_path):
        os.makedirs(directory_path)

    data = json.dumps(obj, indent=indent, ensure_ascii=False)
    with open(file_name, encoding='utf8', mode='w') as fd:
        fd.write(data)

def write_str(file_name, data, encoding='utf8', **kwargs):
    directory_path = os.path.dirname(file_name)
    if directory_path and not os.path.exists(directory_path):
        os.makedirs(directory_path)

    with open(file_name, encoding=encoding, mode='w', **kwargs) as fd:
        fd.write(data)

def read_as_str(file_name):
    with open(file_name, encoding='utf8') as fd:
        return fd.read(-1)