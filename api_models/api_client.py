# coding:utf-8

from __future__ import unicode_literals

import requests


class Query(object):
    def __init__(self, client, model):
        self.client = client
        self.orders = []
        self.model = model
        self.table_name = model._meta.model_name
        self.url = self.client.url + self.table_name + "/"
        self.operation = None
        self.multiple = False
        self.children = []

    def add_ordering(self, column, direction):
        self.orders.append((column, direction))

    def filter(self, field, op, value):
        if field == '_id' and op == '=':
            self.url += str(value) + "/"
        elif field == '_id' and op == 'IN':
            for v in value:
                c = Query(self.client, self.model)
                c.filter(field, '=', v)
                self.children.append(c)
            self.multiple = True

    def fetch(self, offset=0, size=None):
        r = requests.get(self.url)
        d = r.json()
        if isinstance(d, list):
            return d
        return [d]

    def delete(self):
        if self.multiple:
            for c in self.children:
                c.delete()
        else:
            r = requests.delete(self.url)


class Connection(object):
    def __init__(self, url):
        self.url = url

    def query(self, model):
        return Query(self, model)

    def insert(self, table_name, data):
        url = self.url + table_name + "/"
        for d in data:
            if "id" in d:
                r = requests.put(url + str(d['id']) + "/", data=d)
                d = r.json()
            else:
                r = requests.post(url, data=d)
                d = r.json()
                if len(data) == 1:
                    return d['id']