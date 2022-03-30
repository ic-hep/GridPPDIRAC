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
        Standard method for pilot commands, but with git integration.
        """
        repos = self.pp.gitRepos
        if not repos:
            InstallDIRAC.execute(self)
            return
        for url, branch in repos:
            if branch:
                clone_cmd = 'git clone --depth 1 -b %s %s' % (branch, url)
            else:
                clone_cmd = 'git clone --depth 1 %s' % url

            ret_code, _ = self.executeAndGetOutput(clone_cmd)
            if ret_code:
                self.log.error("Failed to git clone DIRAC module '%s'" % url)
                sys.exit(1)
        self.log.info("successfully cloned DIRAC modules from git")

        # We can now look at running the install.
        # Just like the original InstallDIRAC.execture() but add options
        # if needed...
        self._setInstallOptions()

        if os.path.exists("DIRAC"):
            # We got a DIRAC dir from git
            # Set "externals only" install
            self.installOpts.append( '-X' )
            # Create the scripts directory from the git tree...
            scripts_cmd = os.path.join("DIRAC",
                                       "Core",
                                       "scripts",
                                       "dirac-deploy-scripts.py")
            ret_code, _ = self.executeAndGetOutput(scripts_cmd)
            if ret_code:
                self.log.error("Failed to deploy DIRAC scripts")
                sys.exit(1)
            self.log.info("successfully created DIRAC scripts")

        # Finalise the install
        self._locateInstallationScript()
        self._installDIRAC()

