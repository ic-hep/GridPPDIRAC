from DIRAC.ConfigurationSystem.Agent.UsersAndGroups import UsersAndGroups
from DIRAC.Core.Security.MultiVOMSService import MultiVOMSService

class AutoUsersAndGroups(UsersAndGroups):
    def initialize( self ):
        UsersAndGroups.initialize(self)
        self.vomsSrv = MultiVOMSService()
        return S_OK()
    
    def __OLDsyncCSWithVOMS( self ):
        self.__adminMsgs = { 'Errors' : [], 'Info' : [] }

        #Get DIRAC VOMS Mapping
        self.log.info( "Getting DIRAC VOMS mapping" )
        mappingSection = '/Registry/VOMS/Mapping'
        ret = gConfig.getOptionsDict( mappingSection )
        if not ret['OK']:
            self.log.fatal( 'No VOMS to DIRAC Group Mapping Available' )
            return ret
        vomsMapping = ret['Value']
        self.log.info( "There are %s registered voms mappings in DIRAC" % len( vomsMapping ) )

        #Get VOMS VO name
        self.log.info( "Getting VOMS VO name" )
        result = self.vomsSrv.admGetVOName()
        if not ret['OK']:
            self.log.fatal( 'Could not retrieve VOMS VO name' )
        voNameInVOMS = result[ 'Value' ]
        self.log.info( "VOMS VO Name is %s" % voNameInVOMS )

        #Get VOMS roles
        self.log.info( "Getting the list of registered roles in VOMS" )
        result = self.vomsSrv.admListRoles()
        if not ret['OK']:
            self.log.fatal( 'Could not retrieve registered roles in VOMS' )
        rolesInVOMS = result[ 'Value' ]
        self.log.info( "There are %s registered roles in VOMS" % len( rolesInVOMS ) )
        print rolesInVOMS
        rolesInVOMS.append( '' )

        #Map VOMS roles
        vomsRoles = {}
#        rolesInVOMS = [for role in rolesInVOMS]
        for role in rolesInVOMS:
            if role:
                role = "%s/%s" % ( voNameInVOMS, role )
            else:
                role = voNameInVOMS
            #groupsForRole = []
            #for group_name, group_role in vomsMapping.iteritems():
            #    if group_role == role:
            #        groupsForRole.append( group_name )
            groupsForRole = [group_name for group_name, group_role in vomsMapping.iteritems() if group_role == role]
            if groupsForRole:
                vomsRoles[ role ] = { 'Groups' : groupsForRole, 'Users' : [] }
        self.log.info( "DIRAC valid VOMS roles are:\n\t", "\n\t ".join( vomsRoles.keys() ) )

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
        currentUsersDN = {user['DN']: user_nick for user_nick, user in currentUsers.iteritems()}
#        self.__adminMsgs[ 'Info' ].append( "There are %s registered users in DIRAC" % len( currentUsers ) )
        self.log.info( "There are %s registered users in DIRAC" % len( currentUsers ) )

        #Get VOMS user entries
        usersData = {}
        newUserNames = []
        knownUserNames = []
        obsoleteUserNames = []
        self.log.info( "Retrieving usernames..." )
        for vo in self.vomsSrv.getKnownVOs():
            self.log.info( "Getting the list of registered user entries in VOMS for VO %s" % vo )
            result = self.vomsSrv.admListMembers(vo)
            if not ret['OK']:
                self.log.fatal( 'Could not retrieve registered user entries in VOMS for VO %s' % vo )
                continue
            usersInVOMS = result[ 'Value' ]
        #self.__adminMsgs[ 'Info' ].append( "There are %s registered user entries in VOMS" % len( usersInVOMS ) )
            self.log.info( "There are %s registered user entries in VOMS for VO %s" % (len( usersInVOMS ), vo) )

            #Consolidate users by nickname
            usersInVOMS.sort()
            for user in usersInVOMS:
            #for iUPos, user in enumerate(usersInVOMS):
                #userName = ''
                #user = usersInVOMS[ iUPos ]
                userName = currentUsersDN.get(user['DN'].strip(), '')
                #for oldUserName, oldUser in currentUsers.iteritems():
                #    if user[ 'DN' ].strip() in List.fromChar( oldUser[ 'DN' ] ):
                #        userName = oldUserName
                        
                if not userName:
                    result = self.vomsSrv.attGetUserNickname(vo, user[ 'DN' ], user[ 'CA' ])
                    if result[ 'OK' ]:
                        userName = result[ 'Value' ]
                    else:
                        #self.__adminMsgs[ 'Errors' ].append( "Could not retrieve nickname for DN %s" % user[ 'DN' ] )
                        self.log.error( "Could not get nickname for DN %s" % user[ 'DN' ] )
                        userName = user[ 'mail' ][:user[ 'mail' ].find( '@' )]
                if not userName:
                    self.log.error( "Empty nickname for DN %s" % user[ 'DN' ] )
                    #self.__adminMsgs[ 'Errors' ].append( "Empty nickname for DN %s" % user[ 'DN' ] )
                    continue
                #self.log.info( " (%02d%%) Found username %s : %s " % ( ( iUPos * 100 / len( usersInVOMS ) ), userName, user[ 'DN' ] ) )
                #if userName not in usersData:
                #    usersData[ userName ] = { 'DN': [], 'CA': [], 'Email': [], 'Groups' : ['user'] }
                usersData.setdefault(userName, { 'DN': [], 'CA': [], 'Email': [], 'Groups' : ['user'] })
                for key in ( 'DN', 'CA', 'mail' ):
                    value = user[ key ]
                    if not value:
                        continue
                    if key == "mail":
                        List.appendUnique( usersData[ userName ][ 'Email' ], value )
                    else:
                        usersData[ userName ][ key ].append( value.strip() )
                
                if userName not in currentUsers:
                    List.appendUnique( newUserNames, userName )
                else:
                    List.appendUnique( knownUserNames, userName )
        self.log.info( "Finished retrieving usernames" )

        if newUserNames:
            self.log.info( "There are %s new users" % len( newUserNames ) )
        else:
            self.log.info( "There are no new users" )

        #Get the list of users for each group
        result = csapi.listGroups()
        if not result[ 'OK' ]:
            self.log.error( "Could not get the list of groups in DIRAC", result[ 'Message' ] )
            return result
        staticGroups = result[ 'Value' ]
        vomsGroups = []
        self.log.info( "Mapping users in VOMS to groups" )
        for vomsRole in vomsRoles:
            self.log.info( "  Getting users for role %s" % vomsRole )
            groupsForRole = vomsRoles[ vomsRole ][ 'Groups' ]
            vomsMap = vomsRole.split( "Role=" )
            for g in groupsForRole:
                if g in staticGroups:
                    staticGroups.pop( staticGroups.index( g ) )
                else:
                    vomsGroups.append( g )
            if len( vomsMap ) == 1:
                # no Role
                users = usersInVOMS
            else:
                vomsGroup = "Role=".join( vomsMap[:-1] )
                if vomsGroup[-1] == "/":
                    vomsGroup = vomsGroup[:-1]
                vomsRole = "Role=%s" % vomsMap[-1]
                result = self.vomsSrv.admListUsersWithRole( vomsGroup, vomsRole )
                if not result[ 'OK' ]:
                    errorMsg = "Could not get list of users for VOMS %s" % ( vomsMapping[ group ] )
                    self.__adminMsgs[ 'Errors' ].append( errorMsg )
                    self.log.error( errorMsg, result[ 'Message' ] )
                    return result
                users = result['Value']
            numUsersInGroup = 0

            for vomsUser in users:
                for userName in usersData:
                    if vomsUser[ 'DN' ] in usersData[ userName ][ 'DN' ]:
                        numUsersInGroup += 1
                        usersData[ userName ][ 'Groups' ].extend( groupsForRole )
            infoMsg = "There are %s users in group(s) %s for VOMS Role %s" % ( numUsersInGroup, ",".join( groupsForRole ), vomsRole )
            self.__adminMsgs[ 'Info' ].append( infoMsg )
            self.log.info( "  %s" % infoMsg )

        self.log.info( "Checking static groups" )
        staticUsers = []
        for group in staticGroups:
            self.log.info( "  Checking static group %s" % group )
            numUsersInGroup = 0
            result = csapi.listUsers( group )
            if not result[ 'OK' ]:
                self.log.error( "Could not get the list of users in DIRAC group %s" % group , result[ 'Message' ] )
                return result
            for userName in result[ 'Value' ]:
                if userName in usersData:
                    numUsersInGroup += 1
                    usersData[ userName ][ 'Groups' ].append( group )
                else:
                    if group not in vomsGroups and userName not in staticUsers:
                        staticUsers.append( userName )
            infoMsg = "There are %s users in group %s" % ( numUsersInGroup, group )
            self.__adminMsgs[ 'Info' ].append( infoMsg )
            self.log.info( "  %s" % infoMsg )
        if staticUsers:
            infoMsg = "There are %s static users: %s" % ( len( staticUsers ) , ', '.join( staticUsers ) )
            self.__adminMsgs[ 'Info' ].append( infoMsg )
            self.log.info( "%s" % infoMsg )

        for user in currentUsers:
            if user not in usersData and user not in staticUsers:
                self.log.info( 'User %s is no longer valid' % user )
                obsoleteUserNames.append( user )

        #Do the CS Sync
        self.log.info( "Updating CS..." )
        ret = csapi.downloadCSData()
        if not ret['OK']:
            self.log.fatal( 'Can not update from CS', ret['Message'] )
            return ret

        usersWithMoreThanOneDN = {}
        for user in usersData:
            csUserData = dict( usersData[ user ] )
            if len( csUserData[ 'DN' ] ) > 1:
                usersWithMoreThanOneDN[ user ] = csUserData[ 'DN' ]
            result = csapi.describeUsers( [ user ] )
            if result[ 'OK' ]:
                if result[ 'Value' ]:
                    prevUser = result[ 'Value' ][ user ]
                    prevDNs = List.fromChar( prevUser[ 'DN' ] )
                    newDNs = csUserData[ 'DN' ]
                    for DN in newDNs:
                        if DN not in prevDNs:
                            self.__adminMsgs[ 'Info' ].append( "User %s has new DN %s" % ( user, DN ) )
                    for DN in prevDNs:
                        if DN not in newDNs:
                            self.__adminMsgs[ 'Info' ].append( "User %s has lost a DN %s" % ( user, DN ) )
                else:
                    newDNs = csUserData[ 'DN' ]
                    for DN in newDNs:
                        self.__adminMsgs[ 'Info' ].append( "New user %s has new DN %s" % ( user, DN ) )
            for k in ( 'DN', 'CA', 'Email' ):
                csUserData[ k ] = ", ".join( csUserData[ k ] )
            result = csapi.modifyUser( user, csUserData, createIfNonExistant = True )
            if not result[ 'OK' ]:
                self.__adminMsgs[ 'Error' ].append( "Cannot modify user %s: %s" % ( user, result[ 'Message' ] ) )
                self.log.error( "Cannot modify user %s" % user )

        if usersWithMoreThanOneDN:
            self.__adminMsgs[ 'Info' ].append( "\nUsers with more than one DN:" )
            for uwmtod in sorted( usersWithMoreThanOneDN ):
                self.__adminMsgs[ 'Info' ].append( "  %s" % uwmtod )
                self.__adminMsgs[ 'Info' ].append( "    + DN list:" )
                for DN in usersWithMoreThanOneDN[uwmtod]:
                    self.__adminMsgs[ 'Info' ].append( "      - %s" % DN )

        if obsoleteUserNames:
            self.__adminMsgs[ 'Info' ].append( "\nObsolete users:" )
            address = self.am_getOption( 'MailTo', 'graciani@ecm.ub.es' )
            fromAddress = self.am_getOption( 'mailFrom', 'graciani@ecm.ub.es' )
            subject = 'Obsolete LFC Users found'
            body = 'Delete entries into LFC: \n'
            for obsoleteUser in obsoleteUserNames:
                self.log.info( subject, ", ".join( obsoleteUserNames ) )
                body += 'for ' + obsoleteUser + '\n'
                self.__adminMsgs[ 'Info' ].append( "  %s" % obsoleteUser )
            self.log.info( "Deleting %s users" % len( obsoleteUserNames ) )
            NotificationClient().sendMail( address, 'UsersAndGroupsAgent: %s' % subject, body, fromAddress )
            csapi.deleteUsers( obsoleteUserNames )



        if newUserNames:
            self.__adminMsgs[ 'Info' ].append( "\nNew users:" )
            for newUser in newUserNames:
                self.__adminMsgs[ 'Info' ].append( "  %s" % newUser )
                self.__adminMsgs[ 'Info' ].append( "    + DN list:" )
                for DN in usersData[newUser][ 'DN' ]:
                    self.__adminMsgs[ 'Info' ].append( "      - %s" % DN )
                self.__adminMsgs[ 'Info' ].append( "    + EMail: %s" % usersData[newUser][ 'Email' ] )


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
    
    
    def _syncCSWithVOMS( self ):
        self.__adminMsgs = { 'Errors' : [], 'Info' : [] }

        #Get DIRAC VOMS Mapping
        self.log.info( "Getting DIRAC VOMS mapping" )
        ret = gConfig.getOptionsDict('/Registry/VOMS/Mapping')
        if not ret['OK']:
            self.log.fatal( 'No VOMS to DIRAC Group Mapping Available' )
            return ret
        vomsMapping = ret['Value']
        self.log.info( "There are %s registered voms mappings in DIRAC" % len( vomsMapping ) )

        vos = [vo.strip() for vo in self.am_getOption( "VOList", '' ).split(',') if vo]
        
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
        currentUsersDN = {user['DN']: user_nick for user_nick, user in currentUsers.iteritems()}
#        self.__adminMsgs[ 'Info' ].append( "There are %s registered users in DIRAC" % len( currentUsers ) )
        self.log.info( "There are %s registered users in DIRAC" % len( currentUsers ) )
        vomsRoles = {}
        for vo in vos:
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
                    vomsRoles[ role ] = { 'Groups' : groupsForRole, 'Users' : [] }
            self.log.info( "DIRAC valid VOMS roles are:\n\t", "\n\t ".join( vomsRoles.keys() ) )



            #Get VOMS user entries
            usersData = {}
            newUserNames = []
            knownUserNames = []
            obsoleteUserNames = []
            self.log.info( "Retrieving usernames..." )
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
                if not userName:
                    self.log.error( "Could not get nickname for DN %s" % user[ 'DN' ] )
                    userName = user[ 'mail' ][:user[ 'mail' ].find( '@' )]
                    if not userName:
                        self.log.error( "Empty nickname for DN %s, skipping user..." % user[ 'DN' ] )
                        continue
                #self.log.info( " (%02d%%) Found username %s : %s " % ( ( iUPos * 100 / len( usersInVOMS ) ), userName, user[ 'DN' ] ) )
                #if userName not in usersData:
                #    usersData[ userName ] = { 'DN': [], 'CA': [], 'Email': [], 'Groups' : ['user'] }
                usersData.setdefault(userName, { 'DN': [], 'CA': [], 'Email': [], 'Groups' : ['user'] })
                for key in ( 'DN', 'CA', 'mail' ):
                    value = user[ key ]
                    if not value:
                        continue
                    if key == "mail":
                        List.appendUnique( usersData[ userName ][ 'Email' ], value )
                    else:
                        usersData[ userName ][ key ].append( value.strip() )
                
                if userName not in currentUsers:
                    List.appendUnique( newUserNames, userName )
                else:
                    List.appendUnique( knownUserNames, userName )
        self.log.info( "Finished retrieving usernames" )

        if newUserNames:
            self.log.info( "There are %s new users" % len( newUserNames ) )
        else:
            self.log.info( "There are no new users" )

        #Get the list of users for each group
        result = csapi.listGroups()
        if not result[ 'OK' ]:
            self.log.error( "Could not get the list of groups in DIRAC", result[ 'Message' ] )
            return result
        staticGroups = result[ 'Value' ]
        vomsGroups = []
        self.log.info( "Mapping users in VOMS to groups" )
        for vomsRole, vomsRoleInfo in vomsRoles.iteritems():
            self.log.info( "  Getting users for role %s" % vomsRole )
            groupsForRole = vomsRoleInfo[ 'Groups' ]
            vomsMap = vomsRole.split( "Role=" )
            for g in groupsForRole:
                if g in staticGroups:
                    staticGroups.pop( staticGroups.index( g ) )
                else:
                    vomsGroups.append( g )
            if len( vomsMap ) == 1:
                # no Role
                users = usersInVOMS
            else:
                vomsGroup = "Role=".join( vomsMap[:-1] )
                if vomsGroup[-1] == "/":
                    vomsGroup = vomsGroup[:-1]
                vomsRole = "Role=%s" % vomsMap[-1]
                result = self.vomsSrv.admListUsersWithRole( vomsGroup, vomsRole )
                if not result[ 'OK' ]:
                    errorMsg = "Could not get list of users for VOMS %s" % ( vomsMapping[ group ] )
                    self.log.error( errorMsg, result[ 'Message' ] )
                    return result
                users = result['Value']
            numUsersInGroup = 0

            for vomsUser in users:
                for userName in usersData:
                    if vomsUser[ 'DN' ] in usersData[ userName ][ 'DN' ]:
                        numUsersInGroup += 1
                        usersData[ userName ][ 'Groups' ].extend( groupsForRole )
            infoMsg = "There are %s users in group(s) %s for VOMS Role %s" % ( numUsersInGroup, ",".join( groupsForRole ), vomsRole )
            self.log.info( "  %s" % infoMsg )

        self.log.info( "Checking static groups" )
        staticUsers = []
        for group in staticGroups:
            self.log.info( "  Checking static group %s" % group )
            numUsersInGroup = 0
            result = csapi.listUsers( group )
            if not result[ 'OK' ]:
                self.log.error( "Could not get the list of users in DIRAC group %s" % group , result[ 'Message' ] )
                return result
            for userName in result[ 'Value' ]:
                if userName in usersData:
                    numUsersInGroup += 1
                    usersData[ userName ][ 'Groups' ].append( group )
                else:
                    if group not in vomsGroups and userName not in staticUsers:
                        staticUsers.append( userName )
            infoMsg = "There are %s users in group %s" % ( numUsersInGroup, group )
            self.log.info( "  %s" % infoMsg )
        if staticUsers:
            infoMsg = "There are %s static users: %s" % ( len( staticUsers ) , ', '.join( staticUsers ) )
            self.log.info( "%s" % infoMsg )

        for user in currentUsers:
            if user not in usersData and user not in staticUsers:
                self.log.info( 'User %s is no longer valid' % user )
                obsoleteUserNames.append( user )

        return True
        #Do the CS Sync
        self.log.info( "Updating CS..." )
        ret = csapi.downloadCSData()
        if not ret['OK']:
            self.log.fatal( 'Can not update from CS', ret['Message'] )
            return ret

        usersWithMoreThanOneDN = {}
        for user in usersData:
            csUserData = dict( usersData[ user ] )
            if len( csUserData[ 'DN' ] ) > 1:
                usersWithMoreThanOneDN[ user ] = csUserData[ 'DN' ]
            result = csapi.describeUsers( [ user ] )
            if result[ 'OK' ]:
                if result[ 'Value' ]:
                    prevUser = result[ 'Value' ][ user ]
                    prevDNs = List.fromChar( prevUser[ 'DN' ] )
                    newDNs = csUserData[ 'DN' ]
                    for DN in newDNs:
                        if DN not in prevDNs:
                            self.__adminMsgs[ 'Info' ].append( "User %s has new DN %s" % ( user, DN ) )
                    for DN in prevDNs:
                        if DN not in newDNs:
                            self.__adminMsgs[ 'Info' ].append( "User %s has lost a DN %s" % ( user, DN ) )
                else:
                    newDNs = csUserData[ 'DN' ]
                    for DN in newDNs:
                        self.__adminMsgs[ 'Info' ].append( "New user %s has new DN %s" % ( user, DN ) )
            for k in ( 'DN', 'CA', 'Email' ):
                csUserData[ k ] = ", ".join( csUserData[ k ] )
            result = csapi.modifyUser( user, csUserData, createIfNonExistant = True )
            if not result[ 'OK' ]:
                self.__adminMsgs[ 'Error' ].append( "Cannot modify user %s: %s" % ( user, result[ 'Message' ] ) )
                self.log.error( "Cannot modify user %s" % user )

        if usersWithMoreThanOneDN:
            self.__adminMsgs[ 'Info' ].append( "\nUsers with more than one DN:" )
            for uwmtod in sorted( usersWithMoreThanOneDN ):
                self.__adminMsgs[ 'Info' ].append( "  %s" % uwmtod )
                self.__adminMsgs[ 'Info' ].append( "    + DN list:" )
                for DN in usersWithMoreThanOneDN[uwmtod]:
                    self.__adminMsgs[ 'Info' ].append( "      - %s" % DN )

        if obsoleteUserNames:
            self.__adminMsgs[ 'Info' ].append( "\nObsolete users:" )
            address = self.am_getOption( 'MailTo', 'graciani@ecm.ub.es' )
            fromAddress = self.am_getOption( 'mailFrom', 'graciani@ecm.ub.es' )
            subject = 'Obsolete LFC Users found'
            body = 'Delete entries into LFC: \n'
            for obsoleteUser in obsoleteUserNames:
                self.log.info( subject, ", ".join( obsoleteUserNames ) )
                body += 'for ' + obsoleteUser + '\n'
                self.__adminMsgs[ 'Info' ].append( "  %s" % obsoleteUser )
            self.log.info( "Deleting %s users" % len( obsoleteUserNames ) )
            NotificationClient().sendMail( address, 'UsersAndGroupsAgent: %s' % subject, body, fromAddress )
            csapi.deleteUsers( obsoleteUserNames )



        if newUserNames:
            self.__adminMsgs[ 'Info' ].append( "\nNew users:" )
            for newUser in newUserNames:
                self.__adminMsgs[ 'Info' ].append( "  %s" % newUser )
                self.__adminMsgs[ 'Info' ].append( "    + DN list:" )
                for DN in usersData[newUser][ 'DN' ]:
                    self.__adminMsgs[ 'Info' ].append( "      - %s" % DN )
                self.__adminMsgs[ 'Info' ].append( "    + EMail: %s" % usersData[newUser][ 'Email' ] )


        #result = csapi.commitChanges()
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
    