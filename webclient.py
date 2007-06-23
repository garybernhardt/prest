import os
from httplib import HTTPSConnection
import urllib
import logging
import traceback
import md5
import time

import cjson

from bb.common.util import build_auth_header, TransferSpeed


HOST = 'api.bitbacker.com'
ROOT = ''
RETRIES = 10


logger = logging.getLogger('common.webclient')


class RequestError(RuntimeError):
    def __init__(self, message, status_code=None):
        RuntimeError.__init__(self, message)
        self.status_code = status_code


class WebClient:
    def __init__(self, username, password):
        self.username, self.password = username, password
        self.upload_rate = 0
        self.transfer_speed = TransferSpeed()

    def set_upload_rate(self, rate):
        """Set the upload speed rate in bytes per second"""
        self.upload_rate = rate

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
            headers['content-length'] = len(data)

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
        conn.putrequest(verb, url)

        for key, value in headers.iteritems():
            conn.putheader(key, value)
        conn.endheaders()

        if verb in ('POST', 'PUT'):
            if self.upload_rate:
                BLOCK_SIZE = 512
                offset = 0

                while offset < len(data):
                    # Wait until we're slightly below our set upload rate
                    # XXX: This is a lame way to do this
                    while self.transfer_speed.rate > 0.9 * self.upload_rate:
                        time.sleep(0.01)
                    conn.send(data[offset:offset + BLOCK_SIZE])
                    offset += BLOCK_SIZE
                    self.transfer_speed.update(min(BLOCK_SIZE, len(data)))

            else:
                conn.send(data)
                self.transfer_speed.update(len(data))

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

