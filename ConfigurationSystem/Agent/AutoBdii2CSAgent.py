# pylint: disable=attribute-defined-outside-init, broad-except
"""
Automatic BDII -> DIRAC CS Agent.

The AutoBdii2CSAgent checks the BDII for availability of CE and SE
resources for a given or any configured VO. It detects resources not yet
present in the CS and adds them automatically based of configurable
default parameters.
For the CEs and SEs already present in the CS, the agent is updating
if necessary settings which were changed in the BDII recently
"""
from urlparse import urlparse
from datetime import datetime, date, timedelta
from textwrap import dedent

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Agent.Bdii2CSAgent import Bdii2CSAgent
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from GridPPDIRAC.ConfigurationSystem.private.AutoBDIISEs import update_ses
from GridPPDIRAC.ConfigurationSystem.private.AddResourceAPI import (update_ces,
                                                                    remove_old_ces,
                                                                    find_old_ses,
                                                                    find_htcondor_ces,
                                                                    find_arc_ces)


__RCSID__ = "$Id$"


class AutoBdii2CSAgent(Bdii2CSAgent):
    """
    AutoBdii2CSAgent.

    Automatically updates the CS automatically for CEs and SEs.
    """

    domain = 'LCG'
    country_default = 'xx'

    def initialize(self):
        """
        Initialize.

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
        """
        self.domain = self.am_getOption('Domain', AutoBdii2CSAgent.domain)
        self.country_default = self.am_getOption('CountryCodeDefault', AutoBdii2CSAgent.country_default)
        self.bdii_host = self.am_getOption('BDIIHost', "topbdii.grid.hep.ph.ic.ac.uk:2170")
        self.removeOldCEs = self.am_getOption('RemoveOldCEs', True)
        self.ce_removal_threshold = self.am_getOption('CERemovalThreshold', 5)
        self.banned_ces = self.am_getOption('BannedCEs', [])
        self.banned_ses = self.am_getOption('BannedSEs', [])
        return Bdii2CSAgent.initialize(self)

    def execute(self):
        """General agent execution method."""
        # Update SEs
        ##############################
        if self.processSEs:
            url = urlparse('//%s' % self.bdii_host)
            try:
                update_ses(self.voName,
                           address=(url.hostname, url.port if url.port is not None else 2170),
                           banned_ses=self.banned_ses)
            except Exception:
                self.log.exception("Error while running check for new SEs")

        # Update CEs
        ##############################
        if self.processCEs:
            self.log.notice("Starting Glue1 CE processing")
            try:
                update_ces(voList=self.voName,
                           host=self.bdii_host,
                           domain=self.domain,
                           country_default=self.country_default,
                           banned_ces=self.banned_ces,
                           max_processors=self.am_getOption('FixedMaxProcessors', None))
            except Exception:
                self.log.exception("Error while running check for new CEs")

            # Update HTCondor CEs
            ##############################
            self.log.notice("Processing HTCondor Glue2 CEs")
            try:
                find_htcondor_ces(bdii_host=self.bdii_host)
            except Exception:
                self.log.exception("Error while running check for new HTCondor CEs")

            # Update ARC CEs
            ##############################
            self.log.notice("Processing ARC Glue2 CEs")
            try:
                find_arc_ces(bdii_host=self.bdii_host)
            except Exception:
                self.log.exception("Error while running check for new ARC CEs")

        # Remove old CEs with last_seen > threshold
        ##############################
        if self.removeOldCEs:
            try:
                remove_old_ces(removal_threshold=self.ce_removal_threshold,
                               domain=self.domain,
                               banned_ces=self.banned_ces)
            except Exception as err:
                self.log.error("Error while running removal of old CEs: %s" % err)

        # Email about old SEs with last_seen > threshold
        ##############################
        if self.addressTo and self.addressFrom:
            try:
                old_ses = find_old_ses(notification_threshold=14)
            except Exception as err:
                self.log.error("Failed to get old SEs: %s" % err)
                return S_OK()

            if not old_ses:
                self.log.info('No old SEs found.')
                return S_OK()

            # Check when the last notification was sent
            last_notif = self.am_getOption('LastNotification', None)
            if last_notif is None \
               or (date.today() - datetime.strptime(last_notif, '%d/%m/%Y').date() > timedelta(days=7)):
                # Notification has never been sent, or it has been more than threshold days

                self.log.info('Sending old SE notification (%s).' % old_ses)

                body = dedent("""
                Hi,

                The following SEs haven't been seen for a while and probably need removing:

                {ses}

                Regards,
                DIRAC AutoBDII2CS Agent
                """).lstrip('\n') \
                    .format(ses='\n'.join(' - %s (%s)' % se for se in old_ses))
                result = NotificationClient().sendMail(addresses=self.addressTo,
                                                       subject='GridPP DIRAC Old SE Notification',
                                                       body=body,
                                                       fromAddress=self.addressFrom,
                                                       localAttempt=False)
                if not result['OK']:
                    self.log.error('Failed to send old SEs e-mail: ', result['Message'])

                # Now store the date the LastNotification was sent
                # We store this in both the module options and CS
                today = date.today().strftime('%d/%m/%Y')
                self.am_setOption('LastNotification', today)
                cs = CSAPI()
                cs.initialize()
                cs.setOption(cfgPath(self.am_getModuleParam('section'), 'LastNotification'), today)
                cs.commit()

        return S_OK()
