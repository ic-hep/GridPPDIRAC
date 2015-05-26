#############################################################################
# $HeadURL$
#############################################################################

"""
The AutoBdii2CSAgent checks the BDII for availability of CE and SE
resources for a given or any configured VO. It detects resources not yet
present in the CS and adds them automatically based of configurable
default parameters.
For the CEs and SEs already present in the CS, the agent is updating
if necessary settings which were changed in the BDII recently
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent import Bdii2CSAgent
from GridPPDIRAC.ConfigurationSystem.private.AddResourceAPI import (checkUnusedCEs,
                                                                    checkUnusedSEs,
                                                                    removeOldCEs)


class AutoBdii2CSAgent(Bdii2CSAgent):
    '''
    AutoBdii2CSAgent will update the CS automatically for CEs and
    SEs.
    '''
    domain = 'LCG'
    country_default = 'xx'

    def initialize(self):
        '''
        Initialise method pulls in some extra configuration options
        These include:
        domain            - The Grid domain used to generate
                            the DIRAC site name e.g. LCG
        country_default   - The default country code to use to substitute into
                            the dirac site name
        bdii_host         - The host machine:port from which to ldap query
                            default value = None
                            By default uses the DIRAC built in default
                            DIRAC default = 'lcg-bdii.cern.ch:2170'
        '''
        self.domain = self.am_getOption('Domain', 'LCG')
        self.country_default = self.am_getOption('CountryCodeDefault', 'xx')
        self.bdii_host = self.am_getOption('BDIIHost', None)
        self.removeOldCEs = self.am_getOption('RemoveOldCEs', True)
        self.ce_removal_threshold = self.am_getOption('CERemovalThreshold', 5)
        return Bdii2CSAgent.initialize(self)

    def execute(self):
        """
        General agent execution method
        """
        for vo in self.voName:
            if self.processCEs:
                ## Checking for unused CEs
                result = checkUnusedCEs(vo,
                                        host=self.bdii_host,
                                        domain=self.domain,
                                        country_default=self.country_default)
                if not result['OK']:
                    self.log.error("Error while running check for unused CEs "
                                   "in the VO %s: %s"
                                   % (vo, result['Message']))
                    continue

            if self.processSEs:
                ## Checking for unused SEs
                result = checkUnusedSEs(vo, host=self.bdii_host)
                if not result['OK']:
                    self.log.error("Error while running check for unused SEs "
                                   "in the VO %s: %s"
                                   % (vo, result['Message']))
                    continue
        if self.removeOldCEs:
            result = removeOldCEs(self.ce_removal_threshold, self.domain)
            if not result['OK']:
                self.log.error("Error while running removal of old CEs: "
                               "%s" % result['Message'])

        return S_OK()
