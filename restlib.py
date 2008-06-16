from UserList import UserList
from UserDict import UserDict


def _add_links(node, web_client):
    if isinstance(node, list):
        return [_add_links(item, web_client) for item in node]
    elif isinstance(node, unicode):
        return RestlibUnicode(node, web_client)
    elif isinstance(node, str):
        raise ValueError(
            "restlib doesn't support str; this shouldn't happen!")
    elif isinstance(node, dict):
        return dict((k, _add_links(v, web_client))
                    for k, v in node.iteritems())
    else:
        return node


class Resource:
    def __init__(self, href, representation, web_client):
        self.href = href
        self._link = Link(href, web_client)
        self._web_client = web_client

    def refresh(self):
        self.data = _add_links(self._link.get(), self._web_client)

    @classmethod
    def bookmark(cls, href, web_client, raw=False):
        return Link(href, web_client).get(raw=raw)

    @classmethod
    def construct(cls, href, representation, media_type, web_client):
        if media_type == 'application/json':
            if isinstance(representation, list):
                return ListResource(href, representation, web_client)
            elif isinstance(representation, dict):
                return DictResource(href, representation, web_client)
            else:
                raise ValueError(
                    "JSON resources of type \"%s\" aren't supported" %
                    type(representation))
        else:
            return representation

    def get(self, raw=False):
        return self._link.get(raw=raw)
    def post(self, payload, raw=False):
        return self._link.post(payload, raw=raw)
    def put(self, raw=False):
        return self._link.put(self, raw=raw)
    def delete(self):
        return self._link.delete()


class ListResource(Resource, list):
    def __init__(self, href, representation, web_client):
        list.__init__(self, _add_links(representation, web_client))
        Resource.__init__(self, href, representation, web_client)

    def refresh(self):
        while self:
            self.pop()
        self.extend(_add_links(self._link.get(), self._web_client))


class DictResource(Resource, dict):
    def __init__(self, href, representation, web_client):
        dict.__init__(self, _add_links(representation, web_client))
        Resource.__init__(self, href, representation, web_client)

    def refresh(self):
        self.clear()
        self.update(_add_links(self._link.get(), self._web_client))


class RestlibUnicode(unicode):
    """
    Any string in the JSON data is potentially a link, so all strings in the
    JSON data are returned as instances of this class, which has get, post,
    put, and delete methods.
    """
    def __new__(cls, href, web_client):
        self = unicode.__new__(cls, href)
        self.href = href
        self._web_client = web_client
        return self

    def _build_resource(self, href, media_type, representation):
        return Resource.construct(href,
                                  representation,
                                  media_type,
                                  self._web_client)

    def request(self, verb, href, raw, payload):
        content_type, representation = self._web_client.request(
            verb, href, raw, payload)
        return self._build_resource(href, content_type, representation)

    def get(self, *args, **kwargs):
        # This method uses kwargs for the 'raw' argument instead of a default
        # argument because it also catches varargs in 'args'.  If it was
        # declared as get(self, raw=False, *args), and someone called it with
        # get('some-variable'), then 'some-variable' would come in as 'raw',
        # not as one of the args.

        # The variable substitution will do nothing if there are no variables
        # to substitute
        href = self.href % args
        return self.request('GET', href, kwargs.get('raw', False), None)

    def post(self, payload, raw=False):
        return self.request('POST', self.href, raw, payload)

    def put(self, payload, raw=False):
        return self.request('PUT', self.href, raw, payload)

    def delete(self, raw=False):
        return self.request('DELETE', self.href, raw, None)


class Link(RestlibUnicode):
    """
    This is the same as the RestlibUnicode class, but is used when we know the
    thing we're talking about is actually a link.
    """
    pass

