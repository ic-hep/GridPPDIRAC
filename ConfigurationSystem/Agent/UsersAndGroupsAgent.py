"""
UsersAndGroupsAgent
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Agent.UsersAndGroups import UsersAndGroups
from GridPPDIRAC.ConfigurationSystem.private.UsersAndGroupsAPI import UsersAndGroupsAPI


class UsersAndGroupsAgent(UsersAndGroups):
    '''
    UsersAndGroupsAgent
    
    Automatically takes care of updating the DIRAC CS
    to reflect the user and role state of several VOMS
    servers
    '''
    def initialize(self):
        '''Initialisation'''
        self._uag = UsersAndGroupsAPI()
        return UsersAndGroups.initialize(self)

    def execute(self):
        """
        General agent execution method
        """
        result = self._uag.update_usersandgroups()
        if not result['OK']:
            return result

        #LFC Check
        if self.am_getOption("LFCCheckEnabled", True):
            result = self.checkLFCRegisteredUsers(result['Value'])
            if not result['OK']:
                return result
        return S_OK()
