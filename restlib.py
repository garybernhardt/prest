from UserList import UserList
from UserDict import UserDict


def _add_links(node, web_client):
    if isinstance(node, list):
        return [_add_links(item, web_client) for item in node]

    # A link is a dictionary of the form {'href': '<actual href>'}
    elif isinstance(node, dict) and node.keys() == ['href']:
        return Link(node['href'], web_client)

    elif isinstance(node, dict):
        return dict((k, _add_links(v, web_client))
                    for k, v in node.iteritems())

    else:
        return node


class Resource:
    def __init__(self, href, representation, web_client):
        self.href = href
        self._link = Link(href, web_client)
        self.data = _add_links(representation, web_client)
        self._web_client = web_client

    def refresh(self):
        self.data = _add_links(self._link.get(), self._web_client)

    @classmethod
    def bookmark(cls, href, web_client):
        return Link(href, web_client).get()

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

    get = lambda self, *args, **kwargs: self._link.get(*args, **kwargs)
    put = lambda self, *args, **kwargs: self._link.put(*args, **kwargs)
    post = lambda self, *args, **kwargs: self._link.post(*args, **kwargs)
    delete = lambda self, *args, **kwargs: self._link.delete(*args, **kwargs)


class ListResource(Resource, UserList):
    def __init__(self, *args, **kwargs):
        UserList.__init__(self)
        Resource.__init__(self, *args, **kwargs)
class DictResource(Resource, UserDict):
    def __init__(self, *args, **kwargs):
        UserDict.__init__(self)
        Resource.__init__(self, *args, **kwargs)


class Link(object):
    def __init__(self, href, web_client):
        self.href = href
        self._web_client = web_client

    def _build_resource(self, media_type, representation):
        return Resource.construct(self.href,
                                  representation,
                                  media_type,
                                  self._web_client)

    def get(self, **args):
        return self._build_resource(*self._web_client.get(self.href,
                                                          **args))

    def post(self, **args):
        return self._build_resource(*self._web_client.post(self.href, **args))

    def put(self, **args):
        return self._build_resource(*self._web_client.put(self.href, **args))

    def delete(self, **args):
        return self._build_resource(*self._web_client.delete(self.href,
                                                             **args))

