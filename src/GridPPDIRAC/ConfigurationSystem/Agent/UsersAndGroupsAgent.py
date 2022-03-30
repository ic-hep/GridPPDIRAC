"""
UsersAndGroupsAgent
"""

__RCSID__ = "$Id$"
import os
from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from GridPPDIRAC.ConfigurationSystem.private.UsersAndGroupsAPI import UsersAndGroupsAPI


class UsersAndGroupsAgent(AgentModule):
    '''
    UsersAndGroupsAgent

    Automatically takes care of updating the DIRAC CS
    to reflect the user and role state of several VOMS
    servers
    '''
    def initialize(self):
        '''Initialisation'''
        self._uag = UsersAndGroupsAPI()
        self.am_setOption("PollingTime", 3600 * 6)  # Every 6 hours
        self.proxyLocation = os.path.join(self.am_getWorkDirectory(),
                                          ".volatileId")
        return S_OK()

    def execute(self):
        """
        General agent execution method
        """
        result = self._uag.update_usersandgroups()
        if not result['OK']:
            return result

        # LFC Check
        if self.am_getOption("LFCCheckEnabled", True):
            result = self.checkLFCRegisteredUsers(result['Value'])
            if not result['OK']:
                return result
        return S_OK()
