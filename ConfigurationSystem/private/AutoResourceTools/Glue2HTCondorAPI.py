"""Glue2 HTCondor Automated CS filling module."""
import logging
import re
import shlex
import subprocess
from collections import defaultdict
from datetime import date

from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from ConfigurationSystem import ConfigurationSystem
# from .AutoResourceTools.ConfigurationSystem import ConfigurationSystem

endpoint_ce_regex = re.compile(r"^(?:condor|https)://([^:]+):\d+/?$")
dn_ce_regex = re.compile(r"^.*GLUE2ServiceID=([^,:]+)(?::[0-9]+)?,.*$")
dn_site_regex = re.compile(r"^.*GLUE2DomainID=([^,]+),.*$")
cc_regex = re.compile(r'\.([a-zA-Z]{2})$')


# ########################################################################################


class MockLdap(object):
    """Mock of the ldap connection object."""

    entry_regex = re.compile(r"^dn: ([^\n]*)$\n(.*?)$(?=^\s*$)", re.MULTILINE | re.DOTALL)
    option_regex = re.compile(r"(^[^:]+): (.*)$", re.MULTILINE)
    SCOPE_SUBTREE = None

    def __init__(self, hostname, port):
        self._host = ':'.join((hostname, str(port)))

    @classmethod
    def open(cls, hostname, port):
        """Open connection mock."""
        return cls(hostname, port)

    def search_s(self, base, filterstr, scope=None):
        """
        Mimic the return from the ldap search_s API as not available in DiracOS.

        Args:
            base (str): base
            filterstr (str): filters
            scope (*): unused at this point

        Returns:
            list: list of (dn, attib_dict) for items matching the filterstr
        """
        cmd = "ldapsearch -x -LLL -o ldif-wrap=no -h {host} -b {base!r} {filterstr!r}"
        stdout = subprocess.check_output(shlex.split(cmd.format(host=self._host,
                                                                base=base,
                                                                filterstr=filterstr)))
        return [(dn, dict(MockLdap.option_regex.findall(options)))
                for dn, options in MockLdap.entry_regex.findall(stdout)]

# ########################################################################################


try:
    import ldap
except ImportError:
    ldap = MockLdap


def in_(attrs, iterable):
    if isinstance(attrs, basestring):
        return "(|(" + ')('.join('='.join((attrs, value)) for value in iterable) + "))"

    inner_join = lambda values: ''.join(("(&(",
                                         ')('.join('='.join(filt) for filt in zip(attrs, values)),
                                         "))"))
    return "(|" + ''.join(inner_join(values) for values in iterable) + ")"

def get_endpoints(ldap_conn, domain_id, service_id):
    endpoints = set()
    for dn, attrs in ldap_conn.search_s(base="o=glue",
                                        scope=ldap.SCOPE_SUBTREE,
                                        filterstr="(&(objectClass=GLUE2ComputingEndpoint)"
                                                  "(GLUE2ServiceID:dn:=%s)"
                                                  "(GLUE2DomainID:dn:=%s)"
                                                  "(GLUE2EndpointURL=*))" % (
                                                  service_id, domain_id)):  # * forces the field to exist
        endpoints.add(endpoint_ce_regex.sub(r"\1", attrs["GLUE2EndpointURL"][0]))
    return endpoints


def _get_vos(ldap_conn, config_dict):
    for dn, attrs in ldap_conn.search_s(base="o=glue",
                                        scope=ldap.SCOPE_SUBTREE,
                                        filterstr="(&(objectClass=GLUE2MappingPolicy)" +
                                                  in_(("GLUE2DomainID:dn:",
                                                       "GLUE2ServiceID:dn:"),
                                                      config_dict) +
                                                  "(GLUE2PolicyRule=*))"):
        site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn)
        for ce, info in config_dict[site].iteritems():
            queue = '-'.join((ce, "condor"))
            info["Queues"][queue].setdefault("VO", set()).update({vo.lower().replace("vo:", '')
                                                                  for vo in attrs["GLUE2PolicyRule"]})
        #            .update({attrs["GLUE2PolicyRule"].lower().replace("vo:", '')})
            # Maybe not mocked properly by mockldap
    return config_dict


def _get_os_arch(ldap_conn, config_dict):
    os_map = {"centos": "EL"}
    for dn, attrs in ldap_conn.search_s(base="o=glue",
                                        scope=ldap.SCOPE_SUBTREE,
                                        filterstr="(&(objectClass=GLUE2ExecutionEnvironment)" +
                                                  in_(("GLUE2DomainID:dn:",
                                                       "GLUE2ServiceID:dn:"),
                                                      config_dict) +
                                                  "(GLUE2ExecutionEnvironmentOSName=*)"
                                                  "(GLUE2ExecutionEnvironmentOSVersion=*)"
                                                  "(GLUE2ExecutionEnvironmentPlatform=*))"):

        # Maybe not mocked properly by mockldap
        arch = attrs["GLUE2ExecutionEnvironmentPlatform"][0].lower()
        os_version = attrs["GLUE2ExecutionEnvironmentOSVersion"][0]
        os = attrs["GLUE2ExecutionEnvironmentOSName"][0].lower()
#        arch = attrs["GLUE2ExecutionEnvironmentPlatform"].lower()
#        os_version = attrs["GLUE2ExecutionEnvironmentOSVersion"]
#        os = attrs["GLUE2ExecutionEnvironmentOSName"].lower()
#        os = os_map.get(os, os) + os_version
        os = "EL7"  # This is a temporary fix for above as no standard yet

        site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn)
        for ce, info in config_dict[site].iteritems():
            current_arch = info.get("architecture", '')
            current_os = info.get("OS", '')
            if os > current_os or arch > current_arch:
                info["architecture"] = arch
                info["OS"] = os
    return config_dict


def _get_htcondor_ces(ldap_conn):
    htcondor_ces = defaultdict(dict)
    for dn, attrs in ldap_conn.search_s(base="o=glue",
                                        scope=ldap.SCOPE_SUBTREE,
                                        filterstr="(&(objectClass=GLUE2ComputingManager)"
                                                  "(GLUE2ManagerProductName=HTCondor))"):

        service_id, nsubs = dn_ce_regex.subn(r"\1", dn)
        if nsubs != 1:
            logging.warning("Couldn't scrape service id (CE) from dn: %s", dn)
            continue
        if not service_id:
            logging.warning("Scraped service id (CE) is blank string.")
            continue

        domain_id, nsubs2 = dn_site_regex.subn(r"\1", dn)
        if nsubs != 1:
            logging.warning("Couldn't scrape domain id (site) from dn: %s", dn)
            continue
        if not service_id:
            logging.warning("Scraped domain id (site) is blank string.")
            continue

        max_total_jobs = int(attrs.get('GLUE2ComputingManagerTotalPhysicalCPUs',
                                       attrs.get('GLUE2ComputingManagerTotalLogicalCPUs', [0]))[0])

        for ce in get_endpoints(ldap_conn, domain_id, service_id):
            htcondor_ces[(domain_id, service_id)][ce] = {"CEType": "HTCondorCE",
                                                         "SubmissionMode": "Direct",
                                                         "wnTmpDir": '.',
                                                         "SI00": 3100,
                                                         "MaxRAM": 4096,
                                                         "LastSeen": date.today().strftime('%d/%m/%Y'),
                                                         "UseLocalSchedd": False,
                                                         "DaysToKeepLogs": 2,
                                                         "Queues": {'-'.join((ce, 'condor')): {"SI00": 3100,
                                                                                               "MaxTotalJobs": 5000,  # 4 * (max_total_jobs or 1000),
                                                                                               "MaxWaitingJobs": 5000,  # 2 * (max_total_jobs or 1000),
                                                                                               "maxCPUTime": 7777}}}
    htcondor_ces = _get_vos(ldap_conn, htcondor_ces)
    htcondor_ces = _get_os_arch(ldap_conn, htcondor_ces)
    return htcondor_ces


def _get_country_code(ce, default='xx', mapping=None):
    if mapping is None:
        mapping = {'.gov': 'us',
                   '.edu': 'us',
                   'efda.org': 'uk',
                   'atlas-swt2.org': 'us'}
    ce = ce.strip().lower()
    for key, value in mapping.iteritems():
        if ce.endswith(key):
            return value
    match = cc_regex.search(ce)
    if match is not None:
        return match.groups()[0]
    return default


def update_htcondor_ces(bdii_host=("topbdii.grid.hep.ph.ic.ac.uk", 2170)):
    """
    Update HTCondor CEs from BDII.
    """
    ldap_conn = ldap.open(*bdii_host)
    sites_root = '/Resources/Sites/LCG'
    cfg_system = ConfigurationSystem()
    for (site, _), ce_info in sorted(_get_htcondor_ces(ldap_conn).iteritems()):
        for ce, info in ce_info.iteritems():
            site_path = '.'.join(('LCG', site, _get_country_code(ce)))
            cfg_system.append_unique(cfgPath(sites_root, site_path), "CE", ce)
            for option, value in info.iteritems():
                cfg_system.add(cfgPath(sites_root, site_path, "CEs", ce), option, value)
    cfg_system.commit()


if __name__ == "__main__":
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    # ldap_conn = ldap.open("topbdii.grid.hep.ph.ic.ac.uk", 2170)
    # pprint(dict(_get_htcondor_ces(ldap_conn)))
    update_htcondor_ces()
