"""Glue2 HTCondor Automated CS filling module."""
import logging
import re
import shlex
import subprocess
from collections import defaultdict
from datetime import date

from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
# from ConfigurationSystem import ConfigurationSystem
from .ldaptools import in_, MockLdap as ldap
from .ConfigurationSystem import ConfigurationSystem


endpoint_ce_regex = re.compile(r"^(?:condor|https)://([^:]+):\d+/?$")
dn_ce_regex = re.compile(r"^.*GLUE2ServiceID=([^,]+),.*$")
dn_site_regex = re.compile(r"^.*GLUE2DomainID=([^,]+),.*$")
cc_regex = re.compile(r'\.([a-zA-Z]{2})$')


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
        for ce, info in config_dict[site].items():
            queue = '-'.join((ce, "condor"))
            info["Queues"][queue].setdefault("VO", set()).update({vo.lower().replace("vo:", '')
                                                                  for vo in attrs["GLUE2PolicyRule"]})
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

        arch = attrs["GLUE2ExecutionEnvironmentPlatform"][0].lower()
        os_version = attrs["GLUE2ExecutionEnvironmentOSVersion"][0]
        os = attrs["GLUE2ExecutionEnvironmentOSName"][0].lower()
#        os = os_map.get(os, os) + os_version

        site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn)
        for ce, info in config_dict[site].items():
            os = "EL7"  # This is a temporary fix for above as no standard yet
            current_arch = info.get("architecture", '')
            current_os = info.get("OS", '')
            if ce == "ceprod03.grid.hep.ph.ic.ac.uk":
                os = "EL9"
            if os > current_os or arch > current_arch:
                info["architecture"] = arch
                info["OS"] = os
    return config_dict


def _get_htcondor_ces(ldap_conn, max_processors=None):
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


        num_cores = int(max_processors or 64)
        # default time (HTCondor Glue2 does not advertise time)
        maxCPUTime_default = int(2881) # 2 days + 1 min
        # need to check what get_endpoints actually does
        for ce in get_endpoints(ldap_conn, domain_id, service_id):
            if ce == "lcgce02.phy.bris.ac.uk":
                maxCPUTime_site = int(11520) # 8 days
            elif ce.endswith("pp.rl.ac.uk"):
                maxCPUTime_site = int(4320) # RALPP 3 days
            else:
                maxCPUTime_site = maxCPUTime_default

            htcondor_ces[(domain_id, service_id)][ce] = {"CEType": "HTCondorCE",
                                                         "SubmissionMode": "Direct",
                                                         "wnTmpDir": '.',
                                                         "SI00": 3100,
                                                         "HostRAM": 4096,
                                                         "MaxProcessors": num_cores if num_cores > 1 else None,
                                                         "LastSeen": date.today().strftime('%d/%m/%Y'),
                                                         "UseLocalSchedd": False,
                                                         "DaysToKeepLogs": 2,
                                                         "Queues": {'-'.join((ce, 'condor')): {"VO": set(),
                                                                                               "SI00": 3100,
                                                                                               "MaxTotalJobs": 5000,  # 4 * (max_total_jobs or 1000),
                                                                                               "MaxWaitingJobs": 5000,  # 2 * (max_total_jobs or 1000),
                                                                                               "maxCPUTime": maxCPUTime_site}}}
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
    for key, value in mapping.items():
        if ce.endswith(key):
            return value
    match = cc_regex.search(ce)
    if match is not None:
        return match.groups()[0]
    return default


def update_htcondor_ces(vo_list=None, bdii_host=("topbdii.grid.hep.ph.ic.ac.uk", 2170),
                        banned_ces=None, max_processors=None):
    """
    Update HTCondor CEs from BDII.
    """
    ldap_conn = ldap.open(*bdii_host)
    sites_root = '/Resources/Sites/LCG'
    cfg_system = ConfigurationSystem()
    for (site, _), ce_info in sorted(_get_htcondor_ces(ldap_conn, max_processors).items()):
        for ce, info in ce_info.items():
            if banned_ces is not None and ce in banned_ces:
                continue

            if vo_list is not None:
                logging.debug("Filtering out unwanted VOs from HTCondor CE %s", ce)
                # Filter VOs. first part of if is clever ruse to update in a comprehension (always returns None)
                info["Queues"] = {key: val for key, val in info["Queues"].items()
                                  if (val.update(VO=val['VO'].intersection(vo_list)) or val['VO'])}
            if not info["Queues"]:
                logging.warning("Skipping HTCondor CE %s as it has no queues that support our VOs", ce)
                continue
            # duplicate each queue, so we have a single and an 8 core queue
            old_queues = info["Queues"].copy()
            for queue in old_queues:
                multi_queue = "%s-multi" % queue
                info["Queues"][multi_queue] = info["Queues"][queue].copy()
                info["Queues"][queue]["NumberOfProcessors"] = 1
                info["Queues"][multi_queue]["NumberOfProcessors"] = 8
                info["Queues"][multi_queue]["Tag"] = "MultiProcessor"
                info["Queues"][multi_queue]["RequiredTag"] = "MultiProcessor"
                info["Queues"][multi_queue]["LocalCEType"] = "Pool"
            site_path = '.'.join(('LCG', site, _get_country_code(ce)))
            cfg_system.append_unique(cfgPath(sites_root, site_path), "CE", ce)
            for option, value in info.items():
                cfg_system.add(cfgPath(sites_root, site_path, "CEs", ce), option, value)
    cfg_system.commit()


if __name__ == "__main__":
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    # ldap_conn = ldap.open("topbdii.grid.hep.ph.ic.ac.uk", 2170)
    # pprint(dict(_get_htcondor_ces(ldap_conn)))
    update_htcondor_ces()
