import os
import re
from GridPPDIRAC.Core.Security.MultiVOMSService import MultiVOMSService
from DIRAC import gConfig, gLogger
r = re.compile('(?P<group>.*)/(?P<role>Role=.*)')
cn_sanitiser = re.compile('[^a-z_ ]')
cn_regex=re.compile('/CN=(?P<cn>[^/]*)')
class UsersAndGroupsAPI(object):
    def __init__(self):
        self._vomsSrv = MultiVOMSService()
        
    def dirac_names(self, usersDict, matchstart=''):
        for user in usersDict.itervalues():
            if 'DiracName' in user and user['DiracName'].startswith(matchstart):
                yield user['DiracName']
        
#    def users_dict(self, usersList):
#        usersInVOMS={}
#        for user in usersList:
#            if user.get('DN') not in usersInVOMS:
#                user_nick = dirac_user(user)
#                if user_nick in self.dirac_names(usersInVOMS):
#                    user_nick += str(len([u for u in self.dirac_names(usersInVOMS, matchstart=user_nick)]))
#                    if user_nick in self.dirac_names(usersInVOMS):
#                        logger.error("Can't form a unique nick name for user %s, skipping user..." % user['DN'])
#                        continue
#                if not user_nick:
#                    logger.error( "Empty nickname for DN %s, skipping user..." % user[ 'DN' ] )
#                    continue
#                user['DiracName'] = user_nick
#                
#            mail = user.pop('mail', None)
#            if not mail:  # Catches '' and [] as well as None
#                continue
#            usersInVOMS.setdefault(user['DN'], user)\
#                       .setdefault('Email', set())\
#                       .add(mail)
#        return usersInVOMS

    def dirac_user(self, user):
        dn = user.get('DN')
        if not dn:
            gLogger.error('User has no DN')
            return None
        
        cnmatches = cn_regex.findall(dn)
        if len(cnmatches) == 0:
            gLogger.error('User has no CN field in DN')
            return None
        if len(cnmatches) > 1:
            gLogger.warning('User has more than one CN field in DN, using first...')
        # convert to lower case, remove any non [a-z_ ] chars and replace ' ' with '.'
        return cn_sanitiser.sub('', cnmatches[0].lower()).replace(' ','.')
        
    def something(self):
        result = gConfig.getOptionsDict('/Registry/VOMS/Mapping')
        if not result['OK']:
            self.log.fatal( 'No VOMS to DIRAC Group Mapping Available' )
            return result
        vomsMapping = {v:k for k, v in result['Value'].iteritems()}

        usersInVoms = {}
        groups = {}
        for vo in self._vomsSrv.vos:
            ## Users
            ################################################################
            result = self._vomsSrv.admListMembers(vo)
            if not result['OK']:
                self.log.fatal( 'Could not retrieve registered user entries in VOMS for VO %s' % vo )
                continue
            for user in result['Value']:
                if user.get('DN') not in usersInVOMS:
                    user_nick = dirac_user(user)
                    if user_nick in self.dirac_names(usersInVOMS):
                        user_nick += str(len([u for u in self.dirac_names(usersInVOMS, matchstart=user_nick)]))
                        if user_nick in self.dirac_names(usersInVOMS):
                            gLogger.error("Can't form a unique nick name for user %s, skipping user..." % user['DN'])
                            continue
                    if not user_nick:
                        gLogger.error( "Empty nickname for DN %s, skipping user..." % user[ 'DN' ] )
                        continue
                    user['DiracName'] = user_nick
                    
                mail = user.pop('mail', None)
                if mail:  # Catches '' and [] as well as None
                    usersInVOMS.setdefault(user['DN'], user)\
                               .setdefault('Email', set())\
                               .add(mail)
            
            ## Groups
            ################################################################
            result = self._vomsSrv.admGetVOName(vo)
            if not result['OK']:
                self.log.fatal( 'Could not retrieve VOMS VO name for vo %s'% vo )
                continue
            voNameInVOMS = result[ 'Value' ]
                
            groups = {'%s_user' % voNameInVOMS.strip('/') : set(self.dirac_names(usersInVOMS)) }
            
            result = self._vomsSrv.admListRoles(vo)
            if not result['OK']:
                self.log.fatal( 'Could not retrieve registered roles in VOMS for vo' % vo )
                continue
            rolesInVOMS = (role for role in result[ 'Value' ] if role)
            
            for role in rolesInVOMS:
                dirac_group = vomsMapping.get(os.path.join(voNameInVOMS, role))
                if not dirac_group:
                    gLogger.error("Couldn't find DIRAC group for role %s" % role)
                    continue

                result = self._vomsSrv.admListUsersWithRole( vo, voNameInVOMS, role )
                if not result[ 'OK' ]:
                    gLogger.error("Couldn't list users with role %s" % role)
                    continue
                groups[dirac_group] = set((usersInVOMS[groupUser['DN']]['DiracName']
                                           for groupUser in result['Value']
                                           if groupUser['DN'] in usersInVOMS))
                
        ## End of vo loop
        ###################################################################
        

        ## Updating CS
        ###################################################################
        csapi = CSAPI()
        ret = csapi.listUsers()
        if not ret['OK']:
            self.log.fatal( 'Could not retrieve current list of Users' )
            return ret
        ret = csapi.describeUsers( ret['Value'] )
        if not ret['OK']:
            self.log.fatal( 'Could not retrieve current User description' )
            return ret
        currentUsers = {user['DN']: user for user_nick, user in ret['Value'].iteritems()
                        if user['DN'] and user.setdefault('DiracName', user_nick)}
        
        obsoleteUsers = set(self.dirac_names(currentUsers)) - set(self.dirac_names(usersInVOMS))
        if obsoleteUsers:
            csapi.deleteUsers(obsoleteUsers)
            
        for user in usersInVOMS.itervalues():
            user_nick = user.pop('DiracName', None)
            if not user_nick:
                continue
            user['Email'] = ','.join(user.get('Email', ''))
            result = csapi.modifyUser(user_nick, user, createIfNonExistant = True)
            if not result[ 'OK' ]:
                gLogger.error( "Cannot modify user %s, DN: %s" % (user_nick, user.get('DN')))
                continue
            

if __name__ == '__main__':
    ## for some reason config not loaded properly
    gConfig.loadFile('/opt/dirac/etc/DevelConfig.cfg')
    u=UsersAndGroupsAPI()
    u.something()
