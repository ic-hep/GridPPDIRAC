# pylint: disable=attribute-defined-outside-init, bare-except
"""
Automatic GOCDB -> DIRAC CS Agent.

The AutoVac2CSAgent checks the GOCDB for availability of VAC and vcycle
resources for a given or any configured VO. It detects resources not yet
present in the CS and adds them automatically based of configurable
default parameters.
"""
import re
from datetime import date, datetime, timedelta
from pprint import pformat
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from GridPPDIRAC.ConfigurationSystem.private.AutoResourceTools.ConfigurationSystem import ConfigurationSystem

__RCSID__ = "$Id$"

CN_REGEX = re.compile(r'CN=([^/]*)')
VOKEY_EXTENSION_REGEX = re.compile(r'^PILOT_(SE|DN)_(.*)$')
HOSTS_BASE = "Registry/Hosts"
SITES_BASE = "Resources/Sites"

class AutoVac2CSAgent(AgentModule):
    """
    AutoBdii2CSAgent.

    Automatically updates the CS automatically for CEs and SEs.
    """

    max_cputime_map = {'VAC': 400000, 'CLOUD': 24000000}
    cc_regex = re.compile(r'\.([a-zA-Z]{2})$')
    cc_mappings = {'.gov': 'us',
                   '.edu': 'us',
                   'efda.org': 'uk',
                   'atlas-swt2.org': 'us'}

    def initialize(self, *args, **kwargs):
        """
        Initialize.

        Initialise method pulls in some extra configuration options
        These include:
        VOKeys            - List of VO identifiers
        """
        self.vokeys = self.am_getOption('VOKeys', ['GridPP'])
        self.removal_threshold = self.am_getOption('RemovalThreshold', 5)
        self.gocdb_client = GOCDBClient()
        return S_OK()

    def execute(self):
        """General agent execution method."""
        cfg_system = ConfigurationSystem()
        cfg_system.initialize()

        # Get VAC sites.
        # ##############
        result = self.gocdb_client.getServiceEndpointInfo('service_type', "uk.ac.gridpp.vac")
        if not result['OK']:
            self.log.error("Problem getting GOCDB VAC information")
            return result

        try:
            self.process_gocdb_results(result['Value'], 'VAC', cfg_system)
        except:
            self.log.exception("Problem processing GOCDB VAC information")
            return S_ERROR("Problem processing GOCDB VAC information")

        # Get CLOUD (vcycle) sites.
        # #########################
        result = self.gocdb_client.getServiceEndpointInfo('service_type', "uk.ac.gridpp.vcycle")
        if not result['OK']:
            self.log.error("Problem getting GOCDB CLOUD (vcycle) information")
            return result

        try:
            self.process_gocdb_results(result['Value'], 'CLOUD', cfg_system)
        except:
            self.log.exception("Problem processing GOCDB CLOUD (vcycle) information")
            return S_ERROR("Problem processing GOCDB CLOUD (vcycle) information")

        cfg_system.commit()

        # Remove old hosts/sites
        # ######################
        try:
            self.remove_old(self.removal_threshold)
        except:
            self.log.exception("Problem removing old hosts/sites.")
            return S_ERROR("Problem processing GOCDB CLOUD (vcycle) information")

    def process_gocdb_results(self, services, site_path_prefix, cfg_system, country_default='xx'):
        """
        Process GOCDB results.

        Args:
            services (list): List of services returned from GOCDB query.
            site_path_prefix (str): The CS path prefix (VAC or CLOUD) for the type of
                                    service that we are processing.
            cfg_system (ConfigurationSystem): A ConfigurationSystem instance used to update
                                              the CS.
        """
        for service in services:
            # Resources
            sitename = service.get('SITENAME')
            hostname = service.get('HOSTNAME')
            country_code = AutoVac2CSAgent.extract_cc(hostname) or country_default
            if sitename is None or hostname is None:
                self.log.warn("Missing sitename or hostname for service:\n%s" % pformat(service))
                continue

            site_path = cfgPath(SITES_BASE, site_path_prefix, "%s.%s.%s"
                                % (site_path_prefix, sitename, country_code))
            ce_path = cfgPath(site_path, 'CEs', hostname)
            queue_path = cfgPath(ce_path, 'Queues', 'default')
            cfg_system.add(site_path, 'Name', sitename)
            cfg_system.append_unique(site_path, 'CE', hostname)
            cfg_system.add(ce_path, 'CEType', site_path_prefix.capitalize())
            cfg_system.add(ce_path, 'Architecture', 'x86_64')
            cfg_system.add(ce_path, 'OS', 'EL6')
            cfg_system.add(ce_path, 'LastSeen', date.today().strftime('%d/%m/%Y'))
            cfg_system.add(queue_path, 'maxCPUTime',
                           AutoVac2CSAgent.max_cputime_map.get(site_path_prefix, 'Unknown'))

            for extension in service.get('EXTENSIONS', []):
                match = VOKEY_EXTENSION_REGEX.match(extension.get('KEY', ''))
                if match is None:
                    continue

                extension_key = match.group()
                k, vokey = match.groups()
                if vokey not in self.vokeys:
                    self.log.warn("Extension KEY %s for %s with vokey %s does not belong "
                                  "to a valid vokey: %s"
                                  % (extension_key, sitename, vokey, self.vokeys))
                    continue

                if k == 'SE':
                    se = extension.get('VALUE')
                    if se is None:
                        self.log.warn("No SE value for extension KEY %s" % extension_key)
                        continue
                    cfg_system.append_unique(site_path, 'SE', se)

                # Registry
                elif k == 'DN':
                    dn = extension.get('VALUE', '')
                    if "CN=" not in dn:
                        self.log.warn("For extension KEY %s, Could not find the CN component "
                                      "of DN: %s" % (extension_key, dn))
                        continue
                    cn = max(CN_REGEX.findall(dn), key=len)
                    host_path = cfgPath(HOSTS_BASE, cn)
                    cfg_system.add(host_path, 'DN', dn)
                    cfg_system.add(host_path, 'LastSeen', date.today().strftime('%d/%m/%Y'))
                    cfg_system.add(host_path, 'Properties',
                                   ['GenericPilot', 'LimitedDelegation'])

        return S_OK()

    def remove_old(self, removal_threshold=5):
        """Remove old hosts/sites."""
        cfg_system = ConfigurationSystem()
        result = cfg_system.getCurrentCFG()
        if not result['OK']:
            self.log.error('Could not get current config from the CS')
            raise RuntimeError("Error removing old Resources/Registry.")

        today = date.today()
        removal_threshold = timedelta(days=removal_threshold)

        old_ces = set()
        base_path = '/Resources/Sites'
        for site_type in ('VAC', 'CLOUD'):
            site_type_path = cfgPath(base_path, site_type)
            for site, site_info in result['Value'].getAsDict(base_path).iteritems():
                site_path = cfgPath(site_type_path, site)
                for ce, ce_info in site_info.get('CEs', {}).iteritems():
                    ce_path = cfgPath(site_path, 'CEs', ce)

                    if 'LastSeen' not in ce_info:
                        self.log.warn("No LastSeen info for CE: %s at site: %s" % (ce, site))
                        continue

                    last_seen = datetime.strptime(ce_info['LastSeen'], '%d/%m/%Y').date()
                    delta = today - last_seen
                    if delta > removal_threshold:
                        self.log.warn("Last seen %s:%s %s days ago...removing"
                                      % (site, ce, delta.days))
                        cfg_system.remove(section=ce_path)
                        old_ces.add(ce)

                if old_ces:
                    cfg_system.remove(section=site_path, option='CE', value=old_ces)
                    old_ces.clear()

        host_base = '/Registry/Hosts'
        for host, host_info in result['Value'].getAsDict(host_base).iteritems():
            host_path = cfgPath(host_base, host)
            if 'LastSeen' not in host_info:
                self.log.warn("No LastSeen info for host: %s" % host)
                continue

            last_seen = datetime.strptime(host_info['LastSeen'], '%d/%m/%Y').date()
            delta = today - last_seen
            if delta > removal_threshold:
                self.log.warn("Last seen host %s %s days ago...removing"
                              % (host, delta.days))
                cfg_system.remove(section=host_path)
        cfg_system.commit()
        return S_OK()

    @classmethod
    def extract_cc(cls, ce, cc_mappings=None, cc_regex=None):
        """Extract the 2 character country code from the CE name."""
        if cc_mappings is None:
            cc_mappings = cls.cc_mappings
        if cc_regex is None:
            cc_regex = cls.cc_regex

        ce = ce.strip().lower()
        for key, value in cc_mappings.iteritems():
            if ce.endswith(key):
                return value
        cc = cc_regex.search(ce)
        if cc is not None:
            cc = cc.groups()[0]
        return cc
