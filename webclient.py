import os
import httplib
from httplib import HTTPSConnection
import urllib
import logging
import traceback
import md5
import time
import socket
import zlib
from StringIO import StringIO

import cjson

from bb.common.util import build_auth_header, TransferSpeed


HOST = 'api.bitbacker.com'
ROOT = ''


BLOCKS_PER_SECOND = 16 # Number of send() calls per second when our
                       # uploads are rate limited

# On any failing request, retry 3 times with 1 second between
RETRIES = 3
DELAY = 1


logger = logging.getLogger('common.webclient')


# Set the socket timeout to 5 minutes.
# XXX: Is this safe?  What if the API server is overloaded, and some requests
# actually take more than 5 minutes?  The client will then start retrying
# and make the situation even worse.
socket.setdefaulttimeout(5 * 60)


class RequestError(RuntimeError):
    def __init__(self, status_code, reason, message):
        self.status_code = status_code
        self.reason = reason
        self.message = message
        RuntimeError.__init__(self, '%i %s (%s)' %
                              (self.status_code,
                               self.reason,
                               self.message))


def rate_limited_blocks(source_data, upload_rate):
    """
    Yield blocks of data, coming from the string source_data, at a rate that
    ensures upload_rate is respected.
    """
    stream = StringIO(source_data)
    block_size = upload_rate / BLOCKS_PER_SECOND
    time_per_block = 1.0 / BLOCKS_PER_SECOND

    while True:
        block_start_time = time.time()
        block = stream.read(block_size)
        if not block:
            break
        yield block

        block_elapsed = time.time() - block_start_time
        sleep_time = max(0, time_per_block - block_elapsed)
        time.sleep(sleep_time)


class WebClient:
    def __init__(self, username, password, user_agent):
        self.username, self.password = username, password
        self.user_agent = user_agent
        self.upload_rate = 0
        self.transfer_speed = TransferSpeed()

    def set_upload_rate(self, rate):
        """Set the upload speed rate in bytes per second"""
        self.upload_rate = rate

    def request(self, verb, url, raw_data=False, data=None):
        # We keep the retry count in a list so the inner retry() function can
        # change it
        retries = [RETRIES]

        def retry():
            logger.error(
                ('An exception occurred in %s %s; %i retries left.  '
                 'Traceback:\n%s') %
                (verb,
                 url.encode('utf-8'),
                 retries[0],
                 traceback.format_exc()))
            if retries[0] <= 0:
                raise
            retries[0] -= 1
            time.sleep(DELAY)

        while True:
            try:
                return self.single_request(
                    verb, url, raw_data, data)
            except RequestError, e:
                # Only retry on a status code of 500
                if e.status_code == httplib.INTERNAL_SERVER_ERROR:
                    retry()
                else:
                    raise
            except:
                retry()

    def build_headers(self, verb, url, raw_data, data):
        headers = {
            'accept': 'application/json',
            'accept-encoding': 'gzip',
            'user-agent': self.user_agent}
        if raw_data:
            headers['content-type'] = 'application/octet-stream'
        else:
            headers['content-type'] = 'application/json'

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
        if verb in ('POST', 'PUT') and not raw_data:
            data = cjson.encode(data)
        return data

    def send_request(self, conn, headers, verb, url, data):
        conn.putrequest(verb, url)

        for key, value in headers.iteritems():
            conn.putheader(key, value)
        conn.endheaders()

        if verb in ('POST', 'PUT'):
            if self.upload_rate:
                for block in rate_limited_blocks(data, self.upload_rate):
                    conn.send(block)
                    self.transfer_speed.update(len(block))
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

