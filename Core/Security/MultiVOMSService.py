'''
MultiVOMSService

VOMS SOAP Services for multiple VOs
'''
import os
import ssl
import traceback
ssl._DEFAULT_CIPHERS = 'DEFAULT:!ECDH:!aNULL:!eNULL:!LOW:!EXPORT:!SSLv2'

from suds.client import Client
from DIRAC import gConfig, S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.Locations import (getHostCertificateAndKeyLocation,
                                           getCAsLocation)
from DIRAC.Core.Security.VOMSService import (_processListReturn,
                                             _processListDictReturn)
from GridPPDIRAC.Core.Security.HTTPSClientUtils import HTTPSClientCertTransport


class MultiVOMSService(object):
    '''
    Multiple VO VOMS Service
    '''
    def __init__(self, adminUrls=None):
        '''initialise'''
        adminUrls = adminUrls or {}
        self.__soapClients = {}

        locs = getHostCertificateAndKeyLocation()
        if not locs:
            raise RuntimeError("Cannot find the host cert and key files")
        hostCert, hostKey = locs
        gLogger.info("using host cert: %s" % hostCert)
        gLogger.info("using host key: %s" % hostKey)

        result = gConfig.getSections('/Registry/VOMS/URLs')
        if not result['OK']:
            raise Exception(result['Message'])
        self.__vos = result['Value']
        for vo in self.__vos:
            result = gConfig.getOptionsDict('/Registry/VOMS/URLs/%s' % vo)
            if not result['OK']:
                gLogger.error(result['Message'])
                continue
            url_dict = result['Value']
            if 'VOMSAdmin' not in url_dict:
                gLogger.error("Skipping setting up VOMSService for VO: %s "
                              "as no VOMSAdmin option in config" % vo)
                continue

            retries = 3
            while retries:
                try:
                    admin = adminUrls.get(vo, url_dict['VOMSAdmin'])
                    httpstransport = HTTPSClientCertTransport(hostCert,
                                                              hostKey,
                                                              getCAsLocation())
                    adminClient = Client(admin + '?wsdl',
                                         transport=httpstransport)
                    adminClient.set_options(headers={"X-VOMS-CSRF-GUARD": "1"})
                    compatClient = Client(os.path.join(os.path.dirname(admin),
                                                       'VOMSCompatibility?wsdl'),
                                          transport=HTTPSClientCertTransport(hostCert,
                                                                             hostKey,
                                                                             getCAsLocation()))
                    compatClient.set_options(headers={"X-VOMS-CSRF-GUARD": "1"})
                    self.__soapClients[vo] = {'Admin': adminClient, 'Compat': compatClient}
                    break
                except Exception:
                    gLogger.warn("Failed to connect suds client to VOMSAdmin or VOMSCompatibility URL, retrying...")
                    retries -= 1
            else:
                gLogger.error("Maximum number of retries reached. Skipping "
                              "setting up VOMSService for VO: %s" % vo)
                gLogger.error(traceback.format_exc())
        if not self.__soapClients:
            raise RuntimeError("Couldn't setup ANY SOAP clients")

    @property
    def vos(self):
        '''Return list of VOs'''
        return self.__vos

    def getSuspendedMembers(self, vo):
        voms_users = set(user['DN'] for user in self.__soapClients[vo]['Admin'].service.listMembers())
        voms_valid_users = set(self.__soapClients[vo]['Compat'].service.getGridmapUsers())
        return list(voms_users.difference(voms_valid_users))

    def admListMembers(self, vo):
        '''List VO members'''
        try:
            result = self.__soapClients[vo]['Admin'].service.listMembers()
        except Exception, e:
            return S_ERROR("Error in function listMembers: %s" % str(e))
        if 'listMembersReturn' in dir(result):
            return S_OK(_processListDictReturn(result.listMembersReturn))
        return S_OK(_processListDictReturn(result))

    def admListRoles(self, vo):
        '''List VO Roles'''
        try:
            result = self.__soapClients[vo]['Admin'].service.listRoles()
        except Exception, e:
            return S_ERROR("Error in function listRoles: %s" % str(e))
        if 'listRolesReturn' in dir(result):
            return S_OK(_processListReturn(result.listRolesReturn))
        return S_OK(_processListReturn(result))

    def admListUsersWithRole(self, vo, group, role):
        '''List all users with given role'''
        try:
            result = self.__soapClients[vo]['Admin'].service\
                                                    .listUsersWithRole(group, role)
        except Exception, e:
            return S_ERROR("Error in function listUsersWithRole: %s" % str(e))
        if result is None:
            return S_ERROR("listUsersWithRole SOAP service returned None")
        if 'listUsersWithRoleReturn' in dir(result):
            return S_OK(_processListDictReturn(result.listUsersWithRoleReturn))
        return S_OK(_processListDictReturn(result))

    def admGetVOName(self, vo):
        '''Get VO name from VOMS'''
        try:
            result = self.__soapClients[vo]['Admin'].service.getVOName()
        except Exception, e:
            return S_ERROR("Error in function getVOName: %s" % str(e))
        return S_OK(result)
