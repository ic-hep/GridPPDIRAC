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
                                                                    updateSites,
                                                                    updateSEs)


class AutoBdii2CSAgent(Bdii2CSAgent):
    '''
    AutoBdii2CSAgent will update the CS automatically for CEs and
    SEs.
    '''
    domain = 'LCG'
    country_default = 'xx'
    diracSiteTemplate = '{domain}.{site}.{country}'
    diracSENameTemplate = '{DIRACSiteName}-disk'

    def initialize(self):
        '''
        Initialise method pulls in some extra configuration options
        These include:
        domain            - The Grid domain used to generate
                            the DIRAC site name e.g. LCG
        country_default   - the default country code to use to substitute into
                            the dirac site name
        diracSiteTemplate - The template from which the DIRAC site name is
                            generated:
                            Can use substitutions:
                                  {domain}        - The Grid domain e.g. LCG,
                                  {site}          - The site name
                                  {country}       - The country code e.g. uk,
                                                    default is country_default
                                                    if it cannot be determined
                                                    automatically
        diracSENameTemplate - The template from which the DIRAC SE name is
                              generated.
                              Can use substitutions:
                                  {domain}        - The Grid domain e.g. LCG,
                                  {DIRACSiteName} - The DIRAC site name,
                                  {country}       - The country code e.g. uk,
                                  {gridSE}        - The Grid SE name
        '''
        self.domain = self.am_getOption('Domain', 'LCG')
        self.country_default = self.am_getOption('CountryCodeDefault', 'xx')
        self.diracSiteTemplate = self.am_getOption('diracSiteTemplate',
                                                   '{domain}.{site}.{country}')
        self.diracSENameTemplate = self.am_getOption('diracSENameTemplate',
                                                     '{DIRACSiteName}-disk')
        return Bdii2CSAgent.initialize(self)

    def execute(self):
        """
        General agent execution method
        """
        for vo in self.voName:
            if self.processCEs:
                ## Checking for unused CEs
                ceBdii = None
                result = checkUnusedCEs(vo,
                                        alternative_bdii=self.alternativeBDIIs,
                                        domain=self.domain,
                                        country_default=self.country_default,
                                        diracSiteTemplate=
                                        self.diracSiteTemplate)
                if not result['OK']:
                    self.log.error("Error while running check for unused CEs "
                                   "in the VO %s: %s"
                                   % (vo, result['Message']))
                    continue
                ceBdii = result['Value']

                ## Updating the new CEs in the CS
                result = updateSites(vo, ceBdii)
                if not result['OK']:
                    self.log.error("Error while updating sites for VO %s: %s"
                                   % (vo, result['Message']))
                    continue

            if self.processSEs:
                ## Checking for unused SEs
                seBdii = None
                result = checkUnusedSEs(vo,
                                        alternative_bdii=
                                        self.alternativeBDIIs,
                                        domain=self.domain,
                                        country_default=self.country_default,
                                        diracSENameTemplate=
                                        self.diracSENameTemplate)
                if not result['OK']:
                    self.log.error("Error while running check for unused SEs "
                                   "in the VO %s: %s"
                                   % (vo, result['Message']))
                    continue
                seBdii = result['Value']

                ## Updating the new SEs in the CS
                result = updateSEs(vo, seBdii)
                if not result['OK']:
                    self.log.error("Error while updating SEs for VO %s: %s"
                                   % (vo, result['Message']))
                    continue

        return S_OK()
