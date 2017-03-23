'''
HTTPSClientUtils

Module defining an HTTPS transport which allows for the
passing of client cert and key files.
'''
import urllib2
import httplib
import ssl
from suds.transport.http import HttpTransport


class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    '''
    HTTPSClientAuthHandler

    handler for HTTPS client authenticated connections
    '''
    def __init__(self, cert, key, capath):
        '''initialise'''
        urllib2.HTTPSHandler.__init__(self)
        self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        self._ssl_context.load_cert_chain(cert, key)
        self._ssl_context.load_verify_locations(capath=capath)
        self._ssl_context.verify_mode = ssl.CERT_REQUIRED

    def getConnection(self, host, timeout=300):
        '''
        essentially a factory function which allows for the
        passing of client cert and key files to create an
        authenticated HTTPSConnection
        '''
        return httplib.HTTPSConnection(host,
                                       timeout=timeout,
                                       context=self._ssl_context)

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
    def __init__(self, cert, key, capath, *args, **kwargs):
        '''initialise'''
        HttpTransport.__init__(self, *args, **kwargs)
        self.urlopener = urllib2.build_opener(HTTPSClientAuthHandler(cert,
                                                                     key,
                                                                     capath))
