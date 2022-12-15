"""Glue2 HTCondor Automated CS filling module."""
import logging
import re
import shlex
import subprocess
from collections import defaultdict
from datetime import date
from itertools import islice

from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from GridPPDIRAC.ConfigurationSystem.private.AutoResourceTools.ConfigurationSystem import ConfigurationSystem
from .ldaptools import in_, MockLdap as ldap


endpoint_ce_regex = re.compile(r"^(?:ldap|https)://([^:]+):\d+(?:/arex)?$")
#dn_ce_regex = re.compile(r"^.*GLUE2ServiceID=(urn:ogf:ComputingService:[^,:]+:arex),.*$")
dn_ce_regex = re.compile(r"^.*GLUE2ServiceID=([^,]+).*$")
dn_queue_regex = re.compile(r"^.*GLUE2ShareID=([^,]+).*$")  # Can we combine these for just one cc_regex
dn_ce2_regex = re.compile(r"^.*[,]?GLUE2ServiceID=(?:urn:ogf:ComputingService:)?([^,:_]+)(?:_(?:ES)?ComputingElement|:arex|:\d+)?,.*$")
dn_site_regex = re.compile(r"^.*GLUE2DomainID=([^,]+),.*$")
cc_regex = re.compile(r'\.([a-zA-Z]{2})$')
vo_regex = re.compile(r'^(?:vo:|VO:)?([^:]*)$')


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

        os = attrs["GLUE2ExecutionEnvironmentOSName"][0].lower()
#        arch = attrs["GLUE2ExecutionEnvironmentPlatform"].lower()
#        os_version = attrs["GLUE2ExecutionEnvironmentOSVersion"]
#        os = os_map.get(os, os) + os_version
        os = "EL7"  # This is a temporary fix for above as no standard yet

        site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn)
        for ce, info in config_dict[site].items():
            current_arch = info.get("architecture", '')
            current_os = info.get("OS", '')
            info["architecture"] = "x86_64"
            info["OS"] = os
    return config_dict


def _get_arc_ces(ldap_conn, max_processors=None):
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

        num_cores = int(max_processors or 64)
        arc_ces[(domain_id, service_id)][dn_ce2_regex.subn(r"\1", dn)[0]] = {"CEType": "AREX",
                                                 "SubmissionMode": "Direct",
                                                 "wnTmpDir": '.',
                                                 "HostRAM": 4096,
                                                 "MaxProcessors": num_cores if num_cores > 1 else None,
                                                 "LastSeen": date.today().strftime('%d/%m/%Y'),
                                                 "UseLocalSchedd": False,
                                                 "DaysToKeepLogs": 2,
                                                 "Queues": {}}
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
    for key, value in mapping.items():
        if ce.endswith(key):
            return value
    match = cc_regex.search(ce)
    if match is not None:
        return match.groups()[0]
    return default


def update_arc_ces(vo_list=None, bdii_host=("topbdii.grid.hep.ph.ic.ac.uk", 2170),
                   banned_ces=None, max_processors=None):
    """
    Update ARC CEs from BDII.
    """
    ldap_conn = ldap.open(*bdii_host)
    sites_root = '/Resources/Sites/LCG'
    cfg_system = ConfigurationSystem()
    for (site, _), ce_info in sorted(_get_arc_ces(ldap_conn, max_processors).items()):
        for ce, info in ce_info.items():
            if banned_ces is not None and ce in banned_ces:
                continue
            # Candian Sites
            # These require an extra JDL string in the pilot to set the default queue time & default memory
            if ce.endswith('.ca'):
                info['XRSLExtraString'] = '(wallTime="1440")(memory>="3500")'
            # TODO: RALPP runs AREX off an odd port; this information is in the bdii, awaiting re-write of this module
            # until then: hack (note that RAL-LCG2 ends in gridpp.rl.ac.uk and uses the standard port)
            if ce.endswith('.pp.rl.ac.uk'):
                info['Port'] = 60000
            # end RALPP hack
            if vo_list is not None:
                logging.debug("Filtering out unwanted VOs from CE %s", ce)
                # Filter VOs. first part of if is clever ruse to update in a comprehension (always returns None)
                info["Queues"] = {key: val for key, val in info["Queues"].items()
                                  if (val.update(VO=val['VO'].intersection(vo_list)) or val['VO'])}
            if not info["Queues"]:
                logging.warning("Skipping CE %s as it has no queues that support our VOs", ce)
                continue
            # go forth and multiply
            old_queues = info["Queues"].copy()
            for queue in old_queues:
                # because DIRAC puts the queue name in the rsl, everything after the second hyphen needs to be unchanged
                queue_bits = queue.split('-', 1)
                multi_queue = "%s-multim%s" % (queue_bits[0], queue_bits[1])
                info["Queues"][multi_queue] = info["Queues"][queue].copy()
                info["Queues"][queue]["NumberOfProcessors"] = 1
                info["Queues"][multi_queue]["NumberOfProcessors"] = 8
                multi_tag_string = "MultiProcessor"
                # go out on a limb and assume any queues that contain 'gpu' or 'GPU' are exactly that
                if "gpu" in queue or "GPU" in queue:
                    info["Queues"][queue]["Tag"] = "GPU"
                    info["Queues"][queue]["RequiredTag"] = "GPU"
                    multi_tag_string = "MultiProcessor, GPU"
                # Manchester SKA hack
                if "hep.manchester.ac.uk" in ce and "himem" in queue:
                    multi_tag_string = "MultiProcessor, skatelescope.eu.hmem"
                info["Queues"][multi_queue]["Tag"] = multi_tag_string
                info["Queues"][multi_queue]["RequiredTag"] = multi_tag_string
                info["Queues"][multi_queue]["LocalCEType"] = "Pool"
            site_path = '.'.join(('LCG', site, _get_country_code(ce)))
            cfg_system.append_unique(cfgPath(sites_root, site_path), "CE", ce)
            for option, value in info.items():
                cfg_system.add(cfgPath(sites_root, site_path, "CEs", ce), option, value)
    cfg_system.commit()


def _get_queue_prefix(ldap_conn, config_dict):
    queue_prefix = {}
    for dn, attrs in ldap_conn.search_s(base="o=glue", scope=ldap.SCOPE_SUBTREE,
                                        filterstr="(&(objectClass=GLUE2ComputingManager)" +
                                                  in_(("GLUE2DomainID:dn:",
                                                       "GLUE2ServiceID:dn:"),
                                                      config_dict) +
                                                  "(GLUE2ManagerProductName=*))"):
        site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn)
        queue_prefix[site] = '-'.join(("nordugrid", attrs.get("GLUE2ManagerProductName", ["unknown"])[0]))
    return queue_prefix

def _tidy_time(timeval):
    """ Takes a time (usually a queue length) and tries to convert it to minutes.
        This is done by anything < 500 is assumed to be hours.
        Anything > 25000 is assumed to be seconds.
    """
    if timeval < 500:
        return timeval * 60
    if timeval > 25000:
        return timeval / 60
    return timeval

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
        maxCPUTime = int(attrs.get("GLUE2ComputingShareMaxCPUTime", [2940])[0])
        maxWaitingJobs = int(attrs.get("GLUE2ComputingShareMaxWaitingJobs", [2000])[0])
        # Some sites specifically advertise 0 for Max jobs
        # We'll default this to "2222" so it still works, but we can easily see that
        # it isn't the "2000" default.
        if not maxWaitingJobs:
            maxWaitingJobs = 2222
        queue_id = attrs["GLUE2ShareID"][0]
        queue_name = '-'.join((queue_prefix.get((domain_id, service_id), ''),
                               attrs["GLUE2ComputingShareMappingQueue"][0]))
        queues_dict[domain_id, service_id, queue_id] = queue_name
        config_dict.get((domain_id, service_id), {})\
                   .get(ce, {})\
                   .get('Queues', {})[queue_name] = {"VO": set(),
                                                     "SI00": 3100,
                                                     "maxCPUTime": _tidy_time(maxCPUTime),
                                                     "MaxTotalJobs": maxWaitingJobs,
                                                     "MaxWaitingJobs": maxWaitingJobs}
    return _get_vos(ldap_conn, queues_dict, config_dict)

def dict_chunk(dct, size=1000):
    it = dct.items()
    for pos in range(0, len(dct), size):
        yield {i: j for i, j in islice(it, pos, pos + size)}

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
            try:
                site = dn_site_regex.sub(r"\1", dn), dn_ce_regex.sub(r"\1", dn), dn_queue_regex.sub(r"\1", dn)
                ce = dn_ce2_regex.sub(r"\1", dn)
                vo = attrs["GLUE2PolicyRule"][0]
                if vo_regex.match(vo):
                    config_dict.get((site[0], site[1]), {})\
                               .get(ce, {})\
                               .get("Queues", {})[queues_dict[site]]["VO"].add(vo_regex.sub(r"\1", vo))
            except Exception as err:
                # Something wrong with this site, skip it
                logging.warning("Bad entry for %s: %s", dn, str(err))
                continue

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
