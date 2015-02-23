'''
MultiVOMSService

VOMS SOAP Services for multiple VOs
'''
from collections import namedtuple
from DIRAC import gConfig, S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.SOAPFactory import getSOAPClient
from DIRAC.Core.Security.VOMSService import (_processListReturn,
                                             _processListDictReturn)

SOAPClients = namedtuple('SOAPClients', ('Admin', 'Attributes'))


class MultiVOMSService(object):
    '''
    Multiple VO VOMS Service
    '''
    def __init__(self, adminUrls=None, attributesUrls=None):
        '''initialise'''
        adminUrls = adminUrls or {}
        attributesUrls = attributesUrls or {}
        self.__soapClients = {}
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
            if 'VOMSAttributes' not in url_dict:
                gLogger.error("Skipping setting up VOMSService for VO: %s "
                              "as no VOMSAttributes option in config" % vo)
                continue

            retries = 3
            while retries:
                try:
                    admin = adminUrls.get(vo, url_dict['VOMSAdmin'])
                    attr = attributesUrls.get(vo, url_dict['VOMSAttributes'])
                    clients = SOAPClients(getSOAPClient("%s?wsdl" % admin,
                                                        sslMethod="TLSv1"),
                                          getSOAPClient("%s?wsdl" % attr,
                                                        sslMethod="TLSv1"))
                    clients.Admin\
                           .set_options(headers={"X-VOMS-CSRF-GUARD": "1"})
                    clients.Attributes\
                           .set_options(headers={"X-VOMS-CSRF-GUARD": "1"})
                    self.__soapClients[vo] = clients
                    break
                except Exception:
                    retries -= 1
            else:
                gLogger.error("Maximum number of retries reached. Skipping "
                              "setting up VOMSService for VO: %s" % vo)

    @property
    def vos(self):
        '''Return list of VOs'''
        return self.__vos

    def admListMembers(self, vo):
        '''List VO members'''
        try:
            result = self.__soapClients[vo].Admin.service.listMembers()
        except Exception, e:
            return S_ERROR("Error in function listMembers: %s" % str(e))
        if 'listMembersReturn' in dir(result):
            return S_OK(_processListDictReturn(result.listMembersReturn))
        return S_OK(_processListDictReturn(result))

    def admListRoles(self, vo):
        '''List VO Roles'''
        try:
            result = self.__soapClients[vo].Admin.service.listRoles()
        except Exception, e:
            return S_ERROR("Error in function listRoles: %s" % str(e))
        if 'listRolesReturn' in dir(result):
            return S_OK(_processListReturn(result.listRolesReturn))
        return S_OK(_processListReturn(result))

    def admListUsersWithRole(self, vo, group, role):
        '''List all users with given role'''
        try:
            result = self.__soapClients[vo].Admin\
                                           .service\
                                           .listUsersWithRole(group, role)
        except Exception, e:
            return S_ERROR("Error in function listUsersWithRole: %s" % str(e))
        if 'listUsersWithRoleReturn' in dir(result):
            return S_OK(_processListDictReturn(result.listUsersWithRoleReturn))
        return S_OK(_processListDictReturn(result))

    def admGetVOName(self, vo):
        '''Get VO name from VOMS'''
        try:
            result = self.__soapClients[vo].Admin.service.getVOName()
        except Exception, e:
            return S_ERROR("Error in function getVOName: %s" % str(e))
        return S_OK(result)

    def attGetUserNickname(self, vo, dn, ca):
        '''Get a users nickname'''
        user = self.__soapClients[vo].Attributes.factory.create('ns0:User')
        user.DN = dn
        user.CA = ca
        try:
            result = self.__soapClients[vo].Attributes\
                                           .service\
                                           .listUserAttributes(user)
        except Exception, e:
            return S_ERROR("Error in function getUserNickname: %s" % str(e))
        if 'listUserAttributesReturn' in dir(result):
            return S_OK(result.listUserAttributesReturn[0].value)
        return S_OK(result[0].value)
