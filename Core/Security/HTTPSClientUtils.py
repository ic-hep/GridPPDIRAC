'''
HTTPSClientUtils

Module defining an HTTPS transport which allows for the
passing of client cert and key files.
'''
import urllib2
import httplib
from suds.transport.http import HttpTransport


class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    '''
    HTTPSClientAuthHandler

    handler for HTTPS client authenticated connections
    '''
    def __init__(self, cert, key):
        '''initialise'''
        urllib2.HTTPSHandler.__init__(self)
        self.cert = cert
        self.key = key

    def getConnection(self, host, timeout=300):
        '''
        essentially a factory function which allows for the
        passing of client cert and key files to create an
        authenticated HTTPSConnection
        '''
        return httplib.HTTPSConnection(host,
                                       timeout=timeout,
                                       cert_file=self.cert,
                                       key_file=self.key)

    def https_open(self, req):
        '''
        standard handler method using the getConnection
        factory function instead of a reference to a connection
        type.
        '''
        return self.do_open(self.getConnection, req)


class HTTPSClientCertTransport(HttpTransport):
    '''
    suds HTTPS transport allowing for client cert and
    key files to be passed through
    '''
    def __init__(self, cert, key, *args, **kwargs):
        '''initialise'''
        HttpTransport.__init__(self, *args, **kwargs)
        self.urlopener = urllib2.build_opener(HTTPSClientAuthHandler(cert,
                                                                     key))
