import os
from httplib import HTTPSConnection
import urllib
import logging
import traceback
import md5
import sha
from threading import Lock

import cjson

from bb.common.util import build_auth_header, locked
from bb.common.cache import ChunkCache


HOST = 'api.bitbacker.com'
ROOT = ''
RETRIES = 10


logger = logging.getLogger('common.webclient')


class RequestError(RuntimeError):
    def __init__(self, message, status_code=None):
        RuntimeError.__init__(self, message)
        self.status_code = status_code


class WebClient:
    def __init__(self, username, password, cache_dir):
        self.username, self.password = username, password
        # XXX: the cache doesn't belong here; this object never even uses it.
        if cache_dir is not None:
            self.chunk_cache = ChunkCache(cache_dir)

    def request(self, verb, url, raw_data, data):
        retries = RETRIES
        while True:
            try:
                return self.single_request(
                    verb, url, raw_data, data)
            except:
                logger.error(
                    'An exception occurred in %s %s; %i retries left.  '
                    'Traceback:\n%s' %
                    (verb, url, retries, traceback.format_exc()))
                if retries <= 0:
                    raise
                retries -= 1

    def build_headers(self, verb, url, raw_data, data):
        headers = {
            'content-type': 'application/json',
            'accept': 'application/json'}

        if verb in ('POST', 'PUT'):
            headers['content-md5'] = md5.new(data).hexdigest()

        # Add the authorization header
        if self.username is not None and self.password is not None:
            headers['authorization'] = build_auth_header(
                verb, url, headers, self.username, self.password)

        return headers

    def encode_payload(self, verb, raw_data, data):
        # If we're making a request that has a payload, we need to encode it
        if verb in ('POST', 'PUT'):
            if raw_data:
                data = data['data']
            else:
                data = cjson.encode(data)

        return data

    def send_request(self, conn, headers, verb, url, data):
        if verb == 'GET':
            encoded_args = urllib.urlencode(data)
            if encoded_args:
                url += '?' + encoded_args
            conn.request(verb, url, None, headers)
        elif verb == 'POST':
            conn.request(verb, url, data, headers)
        elif verb == 'PUT':
            conn.request(verb, url, data, headers)
        elif verb == 'DELETE':
            conn.request(verb, url, None, headers)
        else:
            # XXX: Raise a more specific exception
            raise RequestError('Unknown HTTP verb: %s' % verb)

    def read_response(self, conn, raw_data):
        resp = conn.getresponse()
        if resp.status not in (200, 204):
            conn.close()
            raise RequestError(
                '%i %s' % (resp.status, resp.reason), resp.status)

        result = resp.read()
        conn.close()

        if resp.status == 204:
            result = None
        elif raw_data:
            result = result
        else:
            result = cjson.decode(result, all_unicode=True)

        return result

    def single_request(self, verb, url, raw_data, data):
        conn = HTTPSConnection(HOST)

        data = self.encode_payload(verb, raw_data, data)
        headers = self.build_headers(verb, url, raw_data, data)
        url = urllib.quote("%s%s" % (ROOT, url))
        self.send_request(conn, headers, verb, url, data)

        result = self.read_response(conn, raw_data)

        return result

    def get(self, url, raw_data=False, **args):
        return self.request('GET', url, raw_data, args)

    def post(self, url, raw_data=False, **args):
        return self.request('POST', url, raw_data, args)

    def put(self, url, raw_data=False, **args):
        return self.request('PUT', url, raw_data, args)

    def delete(self, url, raw_data=False, **args):
        return self.request('DELETE', url, raw_data, args)

