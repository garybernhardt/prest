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


HOST = 'api.bitbacker.com'
ROOT = ''


BLOCKS_PER_SECOND = 16 # Number of send() calls per second when our
                       # uploads are rate limited


# Failing requests will be retried with exponentially increasing delays. First
# 4 seconds, then 8 seconds, then 16 seconds (= 28 seconds total delay, not
# counting the time spent waiting for the failures).
RETRIES = 3
DELAY = 4


logger = logging.getLogger('common.webclient')


# Set the socket timeout.
# XXX: Is this safe?  What if the API server is overloaded, and some requests
# actually take more than 5 minutes?  The client will then start retrying
# and make the situation even worse.
socket.setdefaulttimeout(15 * 60)


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
    def __init__(self, user_agent):
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
        retry_delay = [DELAY]

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
            retry_delay[0] *= 2
            time.sleep(retry_delay[0])

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

    def read_response(self, conn):
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

        content_type = resp.getheader('Content-Type')
        if resp.status == 204:
            content_type, result = None, None
        elif content_type == 'application/json':
            if resp.getheader('Content-Encoding') == 'gzip':
                result = zlib.decompress(result)
            result = cjson.decode(result, all_unicode=True)
        elif content_type == 'application/octet-stream':
            pass
        else:
            raise ValueError('Unsupported Content-Type: %s' % content_type)

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

        content_type, result = self.read_response(conn)

        return content_type, result


class TransferSpeed(object):
    """
    Usage:
        ts = TransferSpeed()
        ts.update(bytes_sent)
        # (do stuff)
        ts.update(bytes_sent)
        print 'sent at', ts.rate, 'bytes per second'
    The bytes_sent argument should always be relative - it's the number of
    bytes sent since the last update() call.
    """
    WINDOW_SIZE = 3 # seconds

    def __init__(self):
        # samples are of the form (time, bytes_since_last_sample)
        self.samples = []

    def update(self, bytes_sent):
        self.samples.append((time.time(), bytes_sent))
        self._truncate()

    def _truncate(self):
        cutoff_time = time.time() - self.WINDOW_SIZE
        while self.samples and self.samples[0][0] < cutoff_time:
            self.samples.pop(0)

    @property
    def rate(self):
        self._truncate()
        return sum(size for time_, size in self.samples) / self.WINDOW_SIZE

