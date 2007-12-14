import os
from httplib import HTTPSConnection
import urllib
import logging
import traceback
import md5
import time
import socket
import zlib

import cjson

from bb.common.util import build_auth_header, TransferSpeed


HOST = 'api.bitbacker.com'
ROOT = ''


MIN_BLOCK_SIZE = 512 # Minimum block size - if our upload rate is
                     # capped at 1K/s, we don't want to be sending
                     # 62.5 byte packets.
BLOCKS_PER_SECOND = 16 # Number of send() calls per second when our
                       # uploads are rate limited
RATE_LIMIT_SLEEP_TIME = 0.1 # Number of seconds to sleep if we're over
                            # our rate limit


logger = logging.getLogger('common.webclient')


# Set the socket timeout to 2 minutes.
# XXX: Is this safe?  What if the API server is overloaded, and some requests
# actually take more than 2 minutes?  The client will then start retrying
# and make the situation even worse.
socket.setdefaulttimeout(120)


class RequestError(RuntimeError):
    def __init__(self, status_code, reason, message):
        self.status_code = status_code
        self.reason = reason
        self.message = message
        RuntimeError.__init__(self, '%i %s (%s)' %
                              (self.status_code,
                               self.reason,
                               self.message))


class WebClient:
    def __init__(self, username, password, user_agent):
        self.username, self.password = username, password
        self.user_agent = user_agent
        self.upload_rate = 0
        self.transfer_speed = TransferSpeed()

    def set_upload_rate(self, rate):
        """Set the upload speed rate in bytes per second"""
        self.upload_rate = rate

    def request(self, verb, url, raw_data, data):
        # Do 3 retries with 3 seconds between, so 9 seconds total before we
        # actually fail
        retries, delay = 3, 1

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
                time.sleep(delay)

    def build_headers(self, verb, url, raw_data, data):
        headers = {
            'content-type': 'application/json',
            'accept': 'application/json',
            'accept-encoding': 'gzip',
            'user-agent': self.user_agent}

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
                BLOCK_SIZE = max(MIN_BLOCK_SIZE,
                                 self.upload_rate / BLOCKS_PER_SECOND)
                offset = 0

                while offset < len(data):
                    block = data[offset:offset + BLOCK_SIZE]
                    while self.transfer_speed.rate > self.upload_rate:
                        time.sleep(RATE_LIMIT_SLEEP_TIME)
                    conn.send(block)
                    self.transfer_speed.update(len(block))
                    offset += BLOCK_SIZE

            else:
                conn.send(data)
                self.transfer_speed.update(len(data))

    def read_response(self, conn, raw_data):
        resp = conn.getresponse()
        if resp.status not in (200, 204):
            conn.close()
            message = resp.read()
            logger.info(('Raising a RequestError on status code %i, ' +
                         'reason "%s", message "%s"') %
                        (resp.status, resp.reason, message))
            raise RequestError(resp.status, resp.reason, message)

        result = resp.read()
        conn.close()

        if resp.status == 204:
            content_type = None
            result = None
        elif raw_data:
            content_type = 'application/octet-stream'
            result = result
        else:
            if resp.getheader('Content-Encoding') == 'gzip':
                result = zlib.decompress(result)
            content_type = resp.getheader('Content-Type',
                                          'application/octet-stream')
            result = cjson.decode(result, all_unicode=True)

        return content_type, result

    def single_request(self, verb, url, raw_data, data):
        conn = HTTPSConnection(HOST)

        # For a GET, the data is a series of key=value pairs; for
        # anything else, it's a payload
        original_url = url
        url = urllib.quote("%s%s" % (ROOT, url))
        if verb == 'GET' and data:
            url += '?' + '&'.join('%s=%s' % (urllib.quote(key),
                                             urllib.quote(value))
                                  for key, value in data)
            data = ''
        else:
            data = self.encode_payload(verb, raw_data, data)
        headers = self.build_headers(verb, original_url, raw_data, data)
        self.send_request(conn, headers, verb, url, data)

        content_type, result = self.read_response(conn, raw_data)

        return content_type, result

    def get(self, url, raw_data=False, args=None):
        return self.request('GET', url, raw_data, args)

    def post(self, url, raw_data=False, **args):
        return self.request('POST', url, raw_data, args)

    def put(self, url, raw_data=False, **args):
        return self.request('PUT', url, raw_data, args)

    def delete(self, url, raw_data=False, **args):
        return self.request('DELETE', url, raw_data, args)

