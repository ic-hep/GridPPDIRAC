########################################################################
# $Id$
########################################################################
'''
Module for defining custom commands for the GridPP DIRAC
instance.
'''
import os
import sys
from pilotCommands import InstallDIRAC

__RCSID__ = "$Id$"


class GitInstallDIRAC(InstallDIRAC):
    """
    Will install the DIRAC code from git while still
    grabbing and installing the required externals.
    """

    def __init__(self, pilotParams):
        """
        c'tor
        """
        InstallDIRAC.__init__(self, pilotParams)

    def execute(self):
        """
        Standard method for pilot commands
        """
        url = self.pp.gitUrl
        if url in (None, ''):
            InstallDIRAC.execute(self)
            return
        branch = self.pp.gitBranch
        clone_cmd = 'git clone --depth 1 %s' % url
        if branch is not None:
            clone_cmd = 'git clone --depth 1 -b %s %s' % (url, branch)
        deploy_scripts_path = os.path.join(self.pp.rootPath,
                                           'DIRAC',
                                           'Core',
                                           'scripts',
                                           'dirac-deploy-scripts.py')

        ret_code, _ = self.executeAndGetOutput(clone_cmd)
        if ret_code:
            self.log.error("Could not git clone DIRAC installation")
            sys.exit(1)
        self.log.info("successfully cloned DIRAC from git")

        ret_code, _ = self.executeAndGetOutput(deploy_scripts_path)
        if ret_code:
            self.log.error("Could not deploy the DIRAC scripts")
            sys.exit(2)
        self.log.info("successfully deployed DIRAC scripts")

        self.installOpts = ['-X']
        if self.pp.tarballUrl:
            self.installOpts.append('-u %s' % self.pp.tarballUrl)
        self._locateInstallationScript()
        self._installDIRAC()
        self.log.info("successfully installed externals")
