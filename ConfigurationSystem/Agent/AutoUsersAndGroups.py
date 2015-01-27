import re
from DIRAC.ConfigurationSystem.Agent.UsersAndGroups import UsersAndGroups
from DIRAC.Core.Security.MultiVOMSService import MultiVOMSService

cn_regex = re.compile('[^a-z_ ]')

class AutoUsersAndGroups(UsersAndGroups):
    def initialize( self ):
        UsersAndGroups.initialize(self)
        self.vomsSrv = MultiVOMSService()
        return S_OK()
    
    def _syncCSWithVOMS( self ):

        #Get DIRAC VOMS Mapping
        self.log.info( "Getting DIRAC VOMS mapping" )
        ret = gConfig.getOptionsDict('/Registry/VOMS/Mapping')
        if not ret['OK']:
            self.log.fatal( 'No VOMS to DIRAC Group Mapping Available' )
            return ret
        vomsMapping = ret['Value']
        self.log.info( "There are %s registered voms mappings in DIRAC" % len( vomsMapping ) )

        
        #Get DIRAC users
        self.log.info( "Getting the list of registered users in DIRAC" )
        csapi = CSAPI()
        ret = csapi.listUsers()
        if not ret['OK']:
            self.log.fatal( 'Could not retrieve current list of Users' )
            return ret
        currentUsers = ret['Value']

        ret = csapi.describeUsers( currentUsers )
        if not ret['OK']:
            self.log.fatal( 'Could not retrieve current User description' )
            return ret
        currentUsers = ret['Value']
        currentUsersDN = {dn: user_nick for user_nick, user in currentUsers.iteritems() for dn in user['DN']}
        self.log.info( "There are %s registered users in DIRAC" % len( currentUsers ) )
        
        #Get the list of users for each group
        result = csapi.listGroups()
        if not result[ 'OK' ]:
            self.log.error( "Could not get the list of groups in DIRAC", result[ 'Message' ] )
            return result
        staticGroups = result[ 'Value' ]
        
        
        
        #vomsRoles = {}
        
        
        usersData = {}
        #vos = [vo.strip() for vo in self.am_getOption( "VOList", '' ).split(',') if vo]
        #for vo in self.vomsSrv.getKnownVOs():
        for vo in self.vomsSrv.vos:

            #Get VOMS user entries
            #usersData = {}
            #obsoleteUserNames = []
            #Get VOMS user entries
            self.log.info( "Getting the list of registered user entries in VOMS for VO %s" % vo )
            result = self.vomsSrv.admListMembers(vo)
            if not ret['OK']:
                self.log.fatal( 'Could not retrieve registered user entries in VOMS for VO %s' % vo )
                continue
            usersInVOMS = result[ 'Value' ]
            self.log.info( "There are %s registered user entries in VOMS for VO %s" % (len( usersInVOMS ), vo) )

            #Consolidate users by nickname
            usersInVOMS.sort()
            for user in usersInVOMS:
                userName = currentUsersDN.get(user['DN'].strip(), '')
                if not userName:  # new user
                    userDN = dict((entry.split('=') for entry in user['DN'].strip(' /').split('/')))
                    # convert to lower case, remove any non [a-z_ ] chars and replace ' ' with '.'
                    username = cn_regex.sub('', userDN['CN'].lower()).replace(' ','.')
                    if username in usersData:
                        username += str(len([user for user in usersData if user.startswith(username)]))
                    if username in usersData:
                        self.log.error("Can't form a unique nick name for user %s, skipping user" % user['DN'])
                        continue
                    if not userName:
                        self.log.error( "Empty nickname for DN %s, skipping user..." % user[ 'DN' ] )
                        continue
                    self.log.info("New user %s with DN: %s" % (userName, user['DN']))
                
                u = usersData.setdefault(userName, currentUsers.get(userName, {'DN': [],
                                                                               'CA': [],
                                                                               'Email': [],
                                                                               'Groups' : ['user']}))                    
                for key in ('DN', 'CA'):
                    if not user[key]:
                        continue
                    u[key] = list(set(u[key]).add(user[key].strip()))
                if user['mail']:
                    u['Email'] = list(set(u['Email']).add(user['mail'].strip()))
                    
            usersDataDN = {dn: user_nick for user_nick, user in usersData.iteritems() for dn in user['DN']}

## Group stuff
##############################################################
            #Get VOMS VO name
            self.log.info( "Getting VOMS VO name for requested vo %s" % vo)
            result = self.vomsSrv.admGetVOName(vo)
            if not ret['OK']:
                self.log.fatal( 'Could not retrieve VOMS VO name for vo %s'% vo )
                continue
            voNameInVOMS = result[ 'Value' ]
            self.log.info( "VOMS VO Name for vo %s is %s" % (vo, voNameInVOMS) )

            #Get VOMS roles
            #[vo/role1, vo/role2, vo]
            self.log.info( "Getting the list of registered roles in VOMS for vo %s" % vo )
            result = self.vomsSrv.admListRoles(vo)
            if not ret['OK']:
                self.log.fatal( 'Could not retrieve registered roles in VOMS for vo' % vo )
                continue
            rolesInVOMS = result[ 'Value' ]
            self.log.info( "There are %s registered roles in VOMS for vo %s" % (len( rolesInVOMS ), vo) )
            rolesInVOMS = [os.path.join(voNameInVOMS, role) for role in rolesInVOMS if role]
            rolesInVOMS.append(voNameInVOMS)
            print rolesInVOMS
            
            #Map VOMS roles 
            ## {role: {'Groups': [groupnames], 'Users': []}}
            #vomsRoles = {}
            for role in rolesInVOMS:
                groupsForRole = [group_name for group_name, group_role in vomsMapping.iteritems() if group_role == role]
                if groupsForRole:
                    #vomsRoles[ role ] = { 'Groups' : groupsForRole, 'Users' : [] }
                    self.log.info( "  Getting users for role %s" % role )
                    vomsMap = role.split( "Role=" )
                    users = usersInVOMS  # no role
                    if len( vomsMap ) != 1:  # some role
                        vomsGroup = "Role=".join( vomsMap[:-1] ).rstrip('/')
                        vomsRole = "Role=%s" % vomsMap[-1]
                        result = self.vomsSrv.admListUsersWithRole( vo, vomsGroup, vomsRole )
                        if not result[ 'OK' ]:
                            self.log.error( "Could not get list of users for VOMS %s" % ( vomsMapping[ vomsGroup ] ), result[ 'Message' ] )
                            continue
                        users = result['Value']
        
                    for vomsUser in users:
                        user_nick = usersDataDN.get(vomsUser[ 'DN' ])
                        if user_nick:
                            #usersData[user_nick]['Groups'].extend(groupsForRole)
                            usersData[user_nick]['Groups'] = list(set(usersData[user_nick]['Groups']).update(groupsForRole))

## Group stuff
##############################################################    
            
            #obsoleteUserNames = [user for user in currentUsers if user not in usersData]
    
        #Do the CS Sync
        self.log.info( "Updating CS..." )
        ret = csapi.downloadCSData()
        if not ret['OK']:
            self.log.fatal( 'Can not update from CS', ret['Message'] )
            return ret

        #usersWithMoreThanOneDN = {}
        result = csapi.describeUsers( usersData.iterkeys() )
        if not result[ 'OK' ]:
            self.log.error("Cannot describe the users to update.")
            return result
        currentUsers = result['Value']
        for user, csUserData in usersData.iteritems():
            if len( csUserData[ 'DN' ] ) > 1:
                self.log.info( "User %s has more than one DN: %s" % (user, csUserData[ 'DN' ]) )

            if user in currentUsers: ## Existing user
                
                prevDNs = set(currentUsers[ user ]['DN'])
                userDNs = set(csUserData[ 'DN' ])
                
                newDNs = userDNs - prevDNs
                exDNs = prevDNs - userDNs
                if newDNs:
                    self.log.info("User %s has new DN(s) %s" % ( user, list(newDNs) ))
                if exDNs:
                    self.log.info("User %s has lost DN(s) %s" % ( user, list(exDN) ))
            else:  ## new user
                newDNs = csUserData[ 'DN' ]
                if newDNs:
                    self.log.info("New user %s has new DN(s) %s" % ( user, newDNs ))
            for k in ( 'DN', 'CA', 'Email' ):
                csUserData[ k ] = ", ".join( csUserData[ k ] )
            result = csapi.modifyUser( user, csUserData, createIfNonExistant = True )
            if not result[ 'OK' ]:
                self.log.error( "Cannot modify user %s" % user )

                
        #for uwmtod, DNs in usersWithMoreThanOneDN.iteritems():
        #    self.log.info( "User %s has more than one DN: %s" % (uwmtod, DNs) )
        obsoleteUserNames = [user for user in currentUsers if user not in usersData]
        if obsoleteUserNames:
            self.log.info( "Deleting Obsolete LFC Users: %s" % obsoleteUserNames )
            csapi.deleteUsers( obsoleteUserNames )


#        for newuser in newUserNames:
#            self.log.info( "New user %s with DN: %s" % (newuser, usersData[newUser][ 'DN' ]) )


        result = csapi.commitChanges()
        if not result[ 'OK' ]:
            self.log.error( "Could not commit configuration changes", result[ 'Message' ] )
            return result
        self.log.info( "Configuration committed" )

        #LFC Check
        if self.am_getOption( "LFCCheckEnabled", True ):
            result = self.checkLFCRegisteredUsers( usersData )
            if not result[ 'OK' ]:
                return result

        return S_OK()
    
    
if __name__ == '__main__':
    a=AutoUsersAndGroups()
    a._syncCSWithVOMS()
    