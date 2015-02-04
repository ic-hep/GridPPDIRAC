import os
import re
from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from GridPPDIRAC.Core.Security.MultiVOMSService import MultiVOMSService
cn_sanitiser = re.compile('[^a-z_ ]')
cn_regex = re.compile('/CN=(?P<cn>[^/]*)')


class DiracUsers(dict):
    @property
    def DiracNames(self):
        return (user['DiracName'] for user in self.itervalues()
                if 'DiracName' in user)

    def nextValidName(self, pattern):
        count = -1
        r = re.compile('%s(?P<index>[0-9]*?)\Z' % pattern)
        ## faster implementation than max
        for u in self.itervalues():
            match = r.search(u['DiracName'])
            if match:
                # or 0 catches the case with no numbers
                m = int(match.group('index') or 0)
                if m > count:
                    count = m
        if count == -1:
            return pattern
        return pattern + str(count + 1)


class UsersAndGroupsAPI(object):
    def __init__(self):
        self._vomsSrv = MultiVOMSService()

    def dirac_name(self, user):
        ## don't recompute if already exists
        if user.get('DiracName'):
            return user['DiracName']

        dn = user.get('DN')
        if not dn:
            gLogger.error('User has no DN')
            return None

        cnmatches = cn_regex.findall(dn)
        if len(cnmatches) == 0:
            gLogger.error('User has no CN field in DN')
            return None
        if len(cnmatches) > 1:
            # CERN DNs have multiple CN fields (AFS UID, UID No., Name field)
            gLogger.warn('User has >1 CN field in DN, using last...')
        # convert to lower case, remove any non [a-z_ ] chars,
        # strip leading and trailing spaces and replace remaining
        # ' ' with '.'
        return cn_sanitiser.sub('', cnmatches[-1].strip().lower())\
                           .replace(' ', '.')

    def update_usersandgroups(self):
        result = gConfig.getOptionsDict('/Registry/VOMS/Mapping')
        if not result['OK']:
            gLogger.fatal('No DIRAC group to VOMS role mapping available')
            gLogger.fatal('Add options to CS /Registry/VOMS/Mapping like...')
            gLogger.fatal('        <vo>_user  = /<vo>')
            gLogger.fatal('        <vo>_admin = /<vo>/Role=admin')
            return result
        vomsMapping = dict(((v, k) for k, v in result['Value'].iteritems()))

        ## Main VO loop
        usersInVOMS = DiracUsers()
        groupsInVOMS = set()
        for vo in self._vomsSrv.vos:
            ## Get the VO name from VOMS
            result = self._vomsSrv.admGetVOName(vo)
            if not result['OK']:
                gLogger.warn('Could not retrieve VOMS VO name for vo %s, '
                             'skipping...' % vo)
                continue
            voNameInVOMS = result['Value']

            ## Get the default DIRAC user group name from the VOMS role mapping
            default_group = vomsMapping.get(voNameInVOMS)
            if not default_group:
                gLogger.warn('No default group for vo %s in mapping '
                             '/Registry/VOMS/Mapping, expected something '
                             'like %s_user = %s' % (voNameInVOMS,
                                                    voNameInVOMS.strip('/'),
                                                    voNameInVOMS))

            ## Users
            ################################################################
            result = self._vomsSrv.admListMembers(vo)
            if not result['OK']:
                gLogger.warn('Could not retrieve registered user entries in '
                             'VOMS for VO %s, skipping...' % vo)
                continue
            for user in result['Value']:
                ## New user check
                if not usersInVOMS.get(user['DN']):
                    user_nick = self.dirac_name(user)
                    if not user_nick:
                        gLogger.error("Empty nickname for DN %s, skipping "
                                      "user..." % user['DN'])
                        continue
                    ## mangle user nickname if it already exists
                    user['DiracName'] = usersInVOMS.nextValidName(user_nick)

                ## all users
                mail = user.pop('mail', None)
                if mail:  # Catches '' and [] as well as None
                    usersInVOMS.setdefault(user['DN'], user)\
                               .setdefault('Email', set())\
                               .add(mail)
                if default_group:
                    groupsInVOMS.add(default_group)
                    usersInVOMS.setdefault(user['DN'], user)\
                               .setdefault('Groups', set())\
                               .add(default_group)

            ## Groups
            ################################################################
            result = self._vomsSrv.admListRoles(vo)
            if not result['OK']:
                gLogger.warn('Could not retrieve registered roles in VOMS '
                             'for vo' % vo)
                gLogger.warn('Will proceed to add users to any default '
                             'defined groups.')
                result['Value'] = ()
            rolesInVOMS = (role for role in result['Value'] if role)

            for role in rolesInVOMS:
                dirac_group = vomsMapping.get(os.path.join(voNameInVOMS, role))
                if not dirac_group:
                    gLogger.error("Couldn't find DIRAC group for role %s in "
                                  "mapping /Registry/VOMS/Mapping, skipping..."
                                  % role)
                    continue

                result = self._vomsSrv.admListUsersWithRole(vo,
                                                            voNameInVOMS,
                                                            role)
                if not result['OK']:
                    gLogger.error("Couldn't list users with role %s, skipping"
                                  % role)
                    continue
                for groupuser in result['Value']:
                    gdn = groupuser['DN']
                    if gdn in usersInVOMS:
                        groupsInVOMS.add(dirac_group)
                        usersInVOMS[gdn].setdefault('Groups', set())\
                                        .add(dirac_group)

        ## End of vo loop
        ###################################################################

        ## Updating CS
        ###################################################################
        gLogger.info("Updating CS with changes/new entries...")
        csapi = CSAPI()
        ret = csapi.downloadCSData()
        if not ret['OK']:
            self.log.fatal('Can not sync the CS', ret['Message'])
            return ret
        ret = csapi.listUsers()
        if not ret['OK']:
            gLogger.fatal('Could not retrieve current list of Users')
            return ret
        ret = csapi.describeUsers(ret['Value'])
        if not ret['OK']:
            gLogger.fatal('Could not retrieve current User description')
            return ret
        currentUsers = ret['Value']

        obsoleteUsers = set(currentUsers) - set(usersInVOMS.DiracNames)
        if obsoleteUsers:
            gLogger.info("Deleting obsolete users: %s" % obsoleteUsers)
            csapi.deleteUsers(obsoleteUsers)

        ## add groups before users as fails if user belongs
        ## to unknown group. addGroups returns S_ERROR if
        ## group already exists but don't care in that case anyway.
        for group in groupsInVOMS:
            csapi.addGroup(group, {'Users': ''})

        for user in usersInVOMS.itervalues():
            user_nick = user.pop('DiracName', None)
            if not user_nick:
                gLogger.warn('No user nickname for user with DN %s, '
                             'skipping...' % user.get('DN'))
                continue
            user['Email'] = ','.join(user.get('Email', ''))
            user['Groups'] = list(user.get('Groups', []))
            result = csapi.modifyUser(user_nick, user,
                                      createIfNonExistant=True)
            if not result['OK']:
                gLogger.error("Cannot modify user %s, DN: %s, skipping"
                              % (user_nick, user.get('DN')))
                continue

        result = csapi.commitChanges()
        if not result['OK']:
            gLogger.error("Could not commit configuration changes",
                          result['Message'])
            return result
        gLogger.info("Configuration committed")

if __name__ == '__main__':
    ## for some reason config not loaded properly
    #gConfig.loadFile('/opt/dirac/etc/DevelConfig.cfg')
    u = UsersAndGroupsAPI()
    u.update_usersandgroups()
