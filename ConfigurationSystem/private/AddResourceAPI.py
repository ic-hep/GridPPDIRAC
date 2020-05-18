"""API for adding resources to CS."""
from datetime import date, datetime, timedelta
from urlparse import urlparse
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.Grid import ldapSE, ldapService, getBdiiCEInfo
from .AutoResourceTools.utils import get_se_vo_info, get_xrootd_ports
from .AutoResourceTools.ConfigurationSystem import ConfigurationSystem
from .AutoResourceTools.SETypes import SE
from .AutoResourceTools.CETypes import Site
from .AutoResourceTools.Glue2HTCondorAPI import update_htcondor_ces
from .AutoResourceTools.Glue2ARCAPI import update_arc_ces

def find_arc_ces(voList, bdii_host="topbdii.grid.hep.ph.ic.ac.uk:2170"):
    """
    Find and add all ARC CEs defined using Glue2.

    Args:
        bdii_host (str): The BDII host in format <hostname>:<port>

    Raises:
        ValueError: If the BDII host str cannot be split to it's two components (hostname and port).
                    Also if the port part cannot be cast to an integer.
    """
    host = bdii_host.rsplit(':', 1)
    if not len(host) == 2:
        msg = "Host is expected to be of type str in format 'hostname:port'"
        gLogger.error(msg)
        raise ValueError(msg)
    try:
        host[1] = int(host[1])
    except ValueError:
        gLogger.error("Could not cast port '%s' to type int" % host[1])
        raise
    update_arc_ces(vo_list=voList, bdii_host=host)

def find_htcondor_ces(bdii_host="topbdii.grid.hep.ph.ic.ac.uk:2170"):
    """
    Find and add all HTCondor CEs defined using Glue2.

    Args:
        bdii_host (str): The BDII host in format <hostname>:<port>

    Raises:
        ValueError: If the BDII host str cannot be split to it's two components (hostname and port).
                    Also if the port part cannot be cast to an integer.
    """
    host = bdii_host.rsplit(':', 1)
    if not len(host) == 2:
        msg = "Host is expected to be of type str in format 'hostname:port'"
        gLogger.error(msg)
        raise ValueError(msg)
    try:
        host[1] = int(host[1])
    except ValueError:
        gLogger.error("Could not cast port '%s' to type int" % host[1])
        raise
    update_htcondor_ces(bdii_host=host)



def find_old_ses(notification_threshold=14):
    """
    Find old SEs.

    Args:
        notification_threshold (int): Only SEs which were last seen longer ago than
                                      this number of days are returned.
    Returns:
        list: A sorter list of two element tuples. These elements are as follows:
              (se name, last seen date). Both elements are strings and the last seen date
              is in the format '%d/%m/%Y'. This should only contain SEs seen longer ago than
              notification_threshold
    """
    result = ConfigurationSystem().getCurrentCFG()
    if not result['OK']:
        gLogger.error('Could not get current config from the CS')
        raise RuntimeError("Error finding old SEs.")

    old_ses = set()
    today = date.today()
    notification_threshold = timedelta(days=notification_threshold)
    for se, se_info in result['Value'].getAsDict('/Resources/StorageElements').iteritems():
        if 'LastSeen' not in se_info:
            gLogger.warn("No LastSeen info for SE: %s" % se)
            continue

        last_seen_str = se_info['LastSeen']
        last_seen = datetime.strptime(last_seen_str, '%d/%m/%Y').date()
        if today - last_seen > notification_threshold:
            old_ses.add((se, last_seen_str))

    return sorted(old_ses)


def update_ces(voList, domain='LCG', country_default='xx', host=None,
               banned_ces=None, max_processors=None):
    """
    Update the CEs in the Dirac config for certain VO list.

    Args:
        vo (list): Updating CEs associated with these VOs
        domain (str): The domain acts as a root directory to the discovered sites as well as
                      prefixing their Dirac names.
        country_default (str): Country code for the Dirac site name is auto discovered from the sites
                               CE host names. If this auto discovery fails the country code defaults
                               to this value
        host (str): The BDII host
        banned_ces (list): List of banned CEs which will be skipped
        max_processors (str/int): If specified and not None, this overrides the BDII gleaned MaxProcessors
                                  value for a site which is defined for all CEs.
    """
    # Get CE info from BDII
    #  We collect across all VOs to prevent "flip-floping" of CE lists.
    ##############################
    site_details = {}
    for vo in voList:
        result = getBdiiCEInfo(vo, host=host)
        if not result['OK']:
            gLogger.error("Failed to call getBdiiCEInfo(vo=%s, host=%s): %s" % (vo, host, result['Message']))
            raise RuntimeError("getBdiiCEInfo failure.")

        ce_bdii_dict = result['Value']
        if not ce_bdii_dict:
            gLogger.warn("No CEs found in BDII for %s" % vo)

        for site_name, site_info in ce_bdii_dict.iteritems():
            if site_name in site_details:
                site_details[site_name].append(site_info)
            else:
                site_details[site_name] = [site_info]

    # Main update loop
    ##############################
    cfg_system = ConfigurationSystem()
    for site, site_info_lst in sorted(site_details.iteritems()):
        try:
            s = Site(site, site_info_lst, domain, country_default, banned_ces, max_processors)
        except Exception:
            gLogger.warn("Skipping problematic site: %s" % site)
            continue
        s.write(cfg_system, cfgPath('/Resources/Sites', domain))
    cfg_system.commit()


def remove_old_ces(removal_threshold=5, domain='LCG', banned_ces=None):
    """
    Remove old CEs.

    Args:
        removal_threshold (int): Only CEs which were last seen longer ago than
                                      this number of days are removed.
        domain (str): The domain/root directory under which to search for sites.
        banned_ces (list): List of banned CEs which will also be removed
    """
    cfg_system = ConfigurationSystem()
    result = cfg_system.getCurrentCFG()
    if not result['OK']:
        gLogger.error('Could not get current config from the CS')
        raise RuntimeError("Error removing old CEs.")

    old_ces = set()
    today = date.today()
    base_path = cfgPath('/Resources/Sites', domain)
    removal_threshold = timedelta(days=removal_threshold)
    for site, site_info in result['Value'].getAsDict(base_path).iteritems():
        site_path = cfgPath(base_path, site)
        for ce, ce_info in site_info.get('CEs', {}).iteritems():
            ce_path = cfgPath(site_path, 'CEs', ce)

            if 'LastSeen' not in ce_info:
                gLogger.warn("No LastSeen info for CE: %s at site: %s" % (ce, site))
                continue

            last_seen = datetime.strptime(ce_info['LastSeen'], '%d/%m/%Y').date()
            if today - last_seen > removal_threshold\
               or (banned_ces is not None and ce in banned_ces):
                cfg_system.remove(section=ce_path)
                old_ces.add(ce)

        if old_ces:
            cfg_system.remove(section=site_path, option='CE', value=old_ces)
            old_ces.clear()
    cfg_system.commit()

__all__ = ('update_ses', 'find_old_ses', 'update_ces', 'remove_old_ces')

if __name__ == '__main__':
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    update_ses(vo='gridpp', host='lcg-bdii.cern.ch:2170')
    update_ces(vo='gridpp', host='lcg-bdii.cern.ch:2170')
