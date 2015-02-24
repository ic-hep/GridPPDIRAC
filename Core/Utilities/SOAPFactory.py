# $HeadURL$
__RCSID__ = "$Id$"

import suds
import urllib2
from DIRAC.Core.DISET.HTTPDISETConnection import HTTPDISETConnection


class TLSv1_Connection(HTTPDISETConnection):
  def _getkwargs(self):
    return {'sslMethod': 'TLSv1'}

mapping = {'TLSv1': TLSv1_Connection,
           'SSLv3': HTTPDISETConnection}

class DISETHandler( urllib2.HTTPSHandler ):
  def set_connObject(self, o):
    self._connObject = o
  def https_open(self, req):
    return self.do_open( self._connObject, req)
    
class DISETHttpTransport( suds.transport.http.HttpTransport ):
  
  def __init__( self, **kwargs ):
    conntype = kwargs.pop('sslMethod', "SSLv3")
    suds.transport.http.HttpTransport.__init__( self, **kwargs )
    self.handler = DISETHandler()
    self.handler.set_connObject(mapping.get(conntype, HTTPDISETConnection))
    self.urlopener = urllib2.build_opener( self.handler )

    
def getSOAPClient( wsdlLocation, **kwargs ):
  kwargs[ 'transport' ] = DISETHttpTransport(sslMethod=kwargs.pop('sslMethod', "SSLv3"))
  return suds.client.Client( wsdlLocation, **kwargs )
    
