"""Glue2 HTCondor Automated CS filling module."""
import logging
import re
import shlex
import subprocess
from collections import defaultdict
from datetime import date
from itertools import islice

from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from ConfigurationSystem import ConfigurationSystem
# from .AutoResourceTools.ConfigurationSystem import ConfigurationSystem

endpoint_ce_regex = re.compile(r"^(?:ldap|https)://([^:]+):\d+(?:/arex)?$")
#dn_ce_regex = re.compile(r"^.*GLUE2ServiceID=(urn:ogf:ComputingService:[^,:]+:arex),.*$")
dn_ce_regex = re.compile(r"^.*GLUE2ServiceID=([^,]+).*$")
dn_queue_regex = re.compile(r"^.*GLUE2ShareID=([^,]+).*$")  # Can we combine these for just one cc_regex
dn_ce2_regex = re.compile(r"^.*[,]?GLUE2ServiceID=(?:urn:ogf:ComputingService:)?([^,:_]+)(?:_(?:ES)?ComputingElement|:arex|:\d+)?,.*$")
dn_site_regex = re.compile(r"^.*GLUE2DomainID=([^,]+),.*$")
cc_regex = re.compile(r'\.([a-zA-Z]{2})$')
vo_regex = re.compile(r'^(?:vo:|VO:)?([^:]*)$')


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


#try:
#    import ldap
#except ImportError:
ldap = MockLdap


def in_(attrs, iterable):
    if isinstance(attrs, basestring):
        return "(|(" + ')('.join('='.join((attrs, value)) for value in iterable) + "))"

    inner_join = lambda values: ''.join(("(&(",
                                         ')('.join('='.join(filt) for filt in zip(attrs, values)),
                                         "))"))
    return "(|" + ''.join(inner_join(values) for values in iterable) + ")"


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
        os = attrs["GLUE2ExecutionEnvironmentOSName"].lower()
#        arch = attrs["GLUE2ExecutionEnvironmentPlatform"].lower()
#        os_version = attrs["GLUE2ExecutionEnvironmentOSVersion"]
#        os = os_map.get(os, os) + os_version
        os = "EL7"  # This is a temporary fix for above as no standard yet

        site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn)
        for ce, info in config_dict[site].iteritems():
            current_arch = info.get("architecture", '')
            current_os = info.get("OS", '')
            info["architecture"] = "x86_64"
            info["OS"] = os
    return config_dict


def _get_arc_ces(ldap_conn):
    arc_ces = defaultdict(dict)
    for dn, attrs in ldap_conn.search_s(base="o=glue",
                                        scope=ldap.SCOPE_SUBTREE,
                                        filterstr="(&(objectClass=GLUE2ComputingService)"
                                                  "(GLUE2ServiceType=org.nordugrid.arex))"):

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

        arc_ces[(domain_id, service_id)][dn_ce2_regex.subn(r"\1", dn)[0]] = {"CEType": "ARC",
                                                 "SubmissionMode": "Direct",
                                                 "wnTmpDir": '.',
                                                 "SI00": 3100,
                                                 "HostRAM": 4096,
                                                 "MaxProcessors": 8,
                                                 "LastSeen": date.today().strftime('%d/%m/%Y'),
                                                 "UseLocalSchedd": False,
                                                 "DaysToKeepLogs": 2,
                                                 "Queues": {}}
    arc_ces = _get_si00(ldap_conn, arc_ces)
    arc_ces = _get_queues(ldap_conn, arc_ces)
#    arc_ces = _get_vos(ldap_conn, arc_ces)
    arc_ces = _get_os_arch(ldap_conn, arc_ces)
    return arc_ces


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


def update_arc_ces(bdii_host=("topbdii.grid.hep.ph.ic.ac.uk", 2170)):
    """
    Update ARC CEs from BDII.
    """
    ldap_conn = ldap.open(*bdii_host)
    sites_root = '/Resources/Sites/LCG'
    cfg_system = ConfigurationSystem()
    for (site, _), ce_info in sorted(_get_arc_ces(ldap_conn).iteritems()):
        for ce, info in ce_info.iteritems():
            site_path = '.'.join(('LCG', site, _get_country_code(ce)))
            cfg_system.append_unique(cfgPath(sites_root, site_path), "CE", ce)
            for option, value in info.iteritems():
                cfg_system.add(cfgPath(sites_root, site_path, "CEs", ce), option, value)
    cfg_system.commit()

def _get_si00(ldap_conn, config_dict):
    for dn, attrs in ldap_conn.search_s(base="o=glue", scope=ldap.SCOPE_SUBTREE,
                                        filterstr="(&(objectClass=GLUE2Benchmark)" +
                                        in_(("GLUE2DomainID:dn:",
                                             "GLUE2ServiceID:dn:"),
                                              config_dict) +
                                        "(GLUE2BenchmarkValue=*))"):
        site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn)
        ce = dn_ce2_regex.sub(r"\1", dn)
        si00 = int(attrs["GLUE2BenchmarkValue"])
        config_dict.get(site, {}).get(ce, {})["SI00"] = si00

def _get_queue_prefix(ldap_conn, config_dict):
    queue_prefix = {}
    for dn, attrs in ldap_conn.search_s(base="o=glue", scope=ldap.SCOPE_SUBTREE,
                                        filterstr="(&(objectClass=GLUE2ComputingManager)" +
                                                  in_(("GLUE2DomainID:dn:",
                                                       "GLUE2ServiceID:dn:"),
                                                      config_dict) +
                                                  "(GLUE2ManagerProductName=*))"):
        site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn)
        queue_prefix[site] = '-'.join(("nordugrid", attrs.get("GLUE2ManagerProductName", "unknown")))
    return queue_prefix


def _get_queues(ldap_conn, config_dict):

    queue_prefix = _get_queue_prefix(ldap_conn, config_dict)

    queues_dict = {}
    for dn, attrs in ldap_conn.search_s(base="o=glue", scope=ldap.SCOPE_SUBTREE,
                                        filterstr="(&(objectClass=GLUE2ComputingShare)" +
                                        in_(("GLUE2DomainID:dn:",
                                             "GLUE2ServiceID:dn:"),
                                              config_dict) +
                                              "(GLUE2ShareID=*)"+
                                              "(GLUE2ComputingShareMappingQueue=*))"):
        domain_id, service_id = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn)
        ce = dn_ce2_regex.sub(r"\1", dn)
        maxCPUTime = attrs.get("GLUE2ComputingShareMaxCPUTime", 5940)
        maxWaitingJobs = int(attrs.get("GLUE2ComputingShareMaxWaitingJobs", 5328))
        queue_id = attrs["GLUE2ShareID"]
        queue_name = '-'.join((queue_prefix.get((domain_id, service_id), ''),
                               attrs["GLUE2ComputingShareMappingQueue"]))
        queues_dict[domain_id, service_id, queue_id] = queue_name
        config_dict.get((domain_id, service_id), {})\
                   .get(ce, {})\
                   .get('Queues', {})[queue_name] = {"VO": set(),
                                                     "SI00": 0,
                                                     "maxCPUTime": maxCPUTime,
                                                     "MaxTotalJobs": 2 * maxWaitingJobs,
                                                     "MaxWaitingJobs": maxWaitingJobs}
    return _get_vos(ldap_conn, queues_dict, config_dict)

def dict_chunk(dct, size=1000):
    it = dct.iteritems()
    for i in xrange(0, len(dct), size):
        yield {i: j for i, j in islice(it, size)}

def _get_vos(ldap_conn, queues_dict, config_dict):
    for queues_dict_chunk in dict_chunk(queues_dict, 300):
        for dn, attrs in ldap_conn.search_s(base="o=glue",
                                            scope=ldap.SCOPE_SUBTREE,
                                            filterstr="(&(objectClass=GLUE2MappingPolicy)" +
                                                      in_(("GLUE2DomainID:dn:",
                                                           "GLUE2ServiceID:dn:",
                                                           "GLUE2ShareID:dn:"),
                                                          queues_dict_chunk) +
                                                      "(GLUE2PolicyRule=*))"):
            site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn), dn_queue_regex.sub(r"\1", dn)
            ce = dn_ce2_regex.sub(r"\1", dn)
            vo = attrs["GLUE2PolicyRule"]
            if vo_regex.match(vo):
                config_dict.get((site[0], site[1]), {})\
                           .get(ce, {})\
                           .get("Queues", {})[queues_dict[site]]["VO"].add(vo_regex.sub(r"\1", vo))
    return config_dict


if __name__ == "__main__":
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    #ldap_conn = ldap.open("topbdii.grid.hep.ph.ic.ac.uk", 2170)
    #a = ldap_conn.search_s(base="o=glue", scope=ldap.SCOPE_SUBTREE,
    #                       filterstr="(&(objectClass=GLUE2ComputingService)"
    #                       "(Glue2ServiceType=org.nordugrid.arex))")
    #b = ldap_conn.search_s(base="o=glue", scope=ldap.SCOPE_SUBTREE,
    #                       filterstr="(&(objectClass=GLUE2ComputingShare)"
    #                                   "(GLUE2DomainID:dn:=UKI-LT2-QMUL)"
    #                                   "(GLUE2ServiceID:dn:=urn:ogf:ComputingService:arcce02.esc.qmul.ac.uk:arex))")
    #c = ldap_conn.search_s(base="o=glue", scope=ldap.SCOPE_SUBTREE,
    #                       filterstr="(&(objectClass=GLUE2MappingPolicy)"
    #                                   "(GLUE2DomainID:dn:=UKI-LT2-QMUL)"
    #                                   "(GLUE2ServiceID:dn:=urn:ogf:ComputingService:arcce02.esc.qmul.ac.uk:arex))")
    #c = ldap_conn.search_s(base="o=glue", scope=ldap.SCOPE_SUBTREE,
    #                       filterstr="(&(objectClass=GLUE2ComputingManager)"
    #                                   "(GLUE2DomainID:dn:=UKI-LT2-QMUL)"
    #                                   "(GLUE2ServiceID:dn:=urn:ogf:ComputingService:arcce02.esc.qmul.ac.uk:arex))")

    #from pprint import pprint
#    get_queues(ldap_conn)
#    pprint(get_queues(ldap_conn))
    #pprint(b)
    #for i in _get_arc_ces(ldap_conn).items():
    #    pprint(i)
    #pprint(dict(_get_arc_ces(ldap_conn)))
#    for dn, stuff in a:
#        print "dn=", dn
#        print "stuff"
#        pprint(stuff)



    # pprint(dict(_get_htcondor_ces(ldap_conn)))
    update_arc_ces()
