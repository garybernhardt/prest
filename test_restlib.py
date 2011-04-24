from dingus import Dingus, DontCare

from restlib import Resource, Link


class WhenMakingRequests:
    def setup(self):
        self.web_client = Dingus()
        self.web_client.request.return_value = ['application/fake', '']
        self.link = Link('/', self.web_client)

    def should_forward_get_requests_to_web_client(self):
        self.link.get()
        assert self.web_client.calls('request',
                                     'GET',
                                     '/',
                                     False,
                                     None).one()

    def should_forward_put_requests_to_web_client(self):
        self.link.put('putdata')
        assert self.web_client.calls('request',
                                     'PUT',
                                     '/',
                                     False,
                                     'putdata').one()

    def should_forward_post_requests_to_web_client(self):
        self.link.post('postdata')
        assert self.web_client.calls('request',
                                     'POST',
                                     '/',
                                     False,
                                     'postdata').one()

    def should_forward_delete_requests_to_web_client(self):
        self.link.delete()
        assert self.web_client.calls('request',
                                     'DELETE',
                                     '/',
                                     False,
                                     None).one()


class WhenBookmarkingURLs(object):
    def setup(self):
        self.web_client = Dingus()
        self.web_client.request.return_value = ['application/fake', 'value']
        self.resource = Resource.bookmark('/', self.web_client)

    def should_return_representation(self):
        assert self.resource == 'value'


class WhenResourcesAreLinked(object):
    def setup(self):
        self.web_client = Dingus()
        self.web_client.request.return_value = [u'application/json',
                                                {u'foo': u'/foo'}]
        self.root = Resource.bookmark('/', self.web_client)

    def should_be_able_to_follow_links(self):
        child = self.root['foo'].get()
        assert child.href == '/foo'

    def following_a_link_should_make_a_web_client_request(self):
        self.root['foo'].get()
        assert self.web_client.calls('request',
                                     'GET',
                                     '/foo',
                                     False,
                                     None).one()


class WhenLinksAreNestedDeepWithinARepresentation(object):
    def setup(self):
        web_client = Dingus()
        web_client.request.return_value = [u'application/json',
                                           {u'foo': [1, 2, u'/bar']}]
        self.root = Resource.bookmark('/', web_client)

    def should_be_able_to_follow_links(self):
        child = self.root['foo'][2].get()
        assert child.href == '/bar'


class WhenFollowingTemplatedLinks(object):
    def setup(self):
        self.web_client = Dingus()
        self.web_client.request.return_value = ['application/fake', '']
        link = Link('/foo/%s/bar', self.web_client)
        link.get('variable')

    def should_send_requests_to_server(self):
        assert self.web_client.calls(
            'request', 'GET', '/foo/variable/bar', False, None).one()


class WhenModifyingAnExistingResource(object):
    def setup(self):
        self.web_client = Dingus()
        self.web_client.request.return_value = ['application/json',
                                                {'foo': [1, 2]}]
        root = Resource.bookmark('/foo', self.web_client)
        root['foo'].append(3)
        root.put()

    def should_do_a_get_request_to_retrieve_original_version(self):
        assert self.web_client.calls('request',
                                     'GET',
                                     '/foo',
                                     False,
                                     None).one()

    def should_do_a_put_request_to_modify_the_resource(self):
        assert self.web_client.calls('request',
                                     'PUT',
                                     '/foo',
                                     DontCare,
                                     DontCare).one()

    def should_send_modified_representation(self):
        assert self.web_client.calls('request',
                                     DontCare,
                                     DontCare,
                                     DontCare,
                                     {'foo': [1, 2, 3]}).one()

    def should_not_make_any_other_requests(self):
        assert len(self.web_client.calls('request')) == 2

