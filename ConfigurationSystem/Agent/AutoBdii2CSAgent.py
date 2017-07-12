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

from datetime import datetime, date, timedelta

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent import Bdii2CSAgent
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from GridPPDIRAC.ConfigurationSystem.private.AddResourceAPI import (checkUnusedCEs,
                                                                    checkUnusedSEs,
                                                                    removeOldCEs,
                                                                    rebuildSiteLists,
                                                                    findOldSEs)


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
        self.banned_ces = self.am_getOption('BannedCEs', [])
        self.banned_ses = self.am_getOption('BannedSEs', [])
        return Bdii2CSAgent.initialize(self)

    def execute(self):
        """
        General agent execution method
        """
        for vo in self.voName:
            if self.processSEs:
                ## Checking for unused SEs
                result = checkUnusedSEs(vo, host=self.bdii_host,
                                        banned_ses=self.banned_ses)
                if not result['OK']:
                    self.log.error("Error while running check for unused SEs "
                                   "in the VO %s: %s"
                                   % (vo, result['Message']))
                    continue

            if self.processCEs:
                ## Checking for unused CEs
                result = checkUnusedCEs(vo,
                                        host=self.bdii_host,
                                        domain=self.domain,
                                        country_default=self.country_default,
                                        banned_ces=self.banned_ces,
                                        max_processors=self.am_getOption('FixedMaxProcessors', None))
                if not result['OK']:
                    self.log.error("Error while running check for unused CEs "
                                   "in the VO %s: %s"
                                   % (vo, result['Message']))
                    continue

        if self.removeOldCEs:
            result = removeOldCEs(self.ce_removal_threshold, self.domain,
                                  self.banned_ces)
            if not result['OK']:
                self.log.error("Error while running removal of old CEs: "
                               "%s" % result['Message'])

        # Rebuild the CE & SE lists for the CE
        result = rebuildSiteLists(domain=self.domain)
        if not result['OK']:
            self.log.error("Failed to rebuild site lists: %s" % result['Message'])

        # Send notification of old SEs if ail addresses are set
        if self.addressTo and self.addressFrom:
            result = findOldSEs()
            if result['OK']:
                old_ses = result['Value']
                # Don't do anything unless there actually are some old SEs...
                if old_ses:
                    # Check when the last notification was sent
                    last_notif_str = self.am_getOption('LastNotification', None)
                    last_notif = None
                    if last_notif_str:
                        last_notif = datetime.strptime(last_notif_str, '%d/%m/%Y').date()
                    if not last_notif or (date.today() - last_notif > timedelta(days=7)):
                        # Notification has never been sent, or it has been more than threshold days
                        notification = NotificationClient()
                        subject = "GridPP DIRAC Old SE Notification"
                        body =  "Hi,\n"
                        body += "\n"
                        body += "The following SEs haven't been seen for a while and probably need removing:\n"
                        body += "\n"
                        for se_name, last_seen in old_ses:
                            body += " - %s (%s)\n" % (se_name, last_seen)
                        body += "\n"
                        body += "Regards,\n"
                        body += "DIRAC AutoBDII2CS Agent\n"
                        self.log.info('Sending old SE notification (%s).' % old_ses)
                        result = notification.sendMail(self.addressTo, subject, body,
                                                       self.addressFrom, localAttempt = False)
                        if not result['OK']:
                            self.log.error('Failed to send old SEs e-mail: ', result['Message'])

                        # Now store the date the LastNotification was sent
                        # We store this in both the module options and CS
                        date_str = date.today().strftime('%d/%m/%Y')
                        self.am_setOption('LastNotification', date_str)
                        conf_path = "%s/%s" % (self.am_getModuleParam('section'), 'LastNotification')
                        cs = CSAPI()
                        cs.initialize()
                        cs.setOption(conf_path, date_str)
                        cs.commit()
                    else:
                        self.log.info('Not sending old SE notification yet.')
                else:
                    self.log.info('No old SEs found.')
            else:
                self.log.error("Failed to get old SEs.")

        return S_OK()

