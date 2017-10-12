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


def update_ses(vo, host=None, banned_ses=None):
    """
    Update the SEs in the Dirac config for certain VO.

    Args:
        vo (str): Updating SEs associated with this VO
        host (str): The BDII host
        banned_ses (list): List of banned SEs which will be skipped
    """
    # Get SEs from DBII
    ##############################
    result = ldapSE('*', vo=vo, host=host)
    if not result['OK']:
        gLogger.error("Failed to call ldapSE('*', vo=%s, host=%s): %s" % (vo, host, result['Message']))
        raise RuntimeError("ldapSE failure.")

    ses = {se: se_info for se, se_info
           in ((i.get('GlueSEUniqueID', ''), i) for i in result['Value'])  # pylint: disable=no-member
           if '.' in se}
    if not ses:
        gLogger.warn("No SEs found in BDII")

    # Get dict of storage SRMs: endpoints
    ##############################
    result = ldapService(serviceType='SRM', vo=vo, host=host)
    if not result['OK']:
        gLogger.error("Failed to call ldapService(serviceType='SRM', vo=%s, host=%s): %s"
                      % (vo, host, result['Message']))
        raise RuntimeError("ldapService Failure.")

    srms = {hostname: endpoint for hostname, endpoint
            in ((urlparse(i.get('GlueServiceEndpoint', '')).hostname, i) for i in result['Value'])  # pylint: disable=no-member
            if hostname in ses}

    # Get dict of SE: VO info paths
    ##############################
    vo_info = get_se_vo_info(vo, host=host)
    if not vo_info:
        gLogger.warn("No SE -> VO info path mapping for any SE.")


    cfg_system = ConfigurationSystem()
    # Get existing SEs hosts
    ##############################
    dirac_ses = {}
    result = cfg_system.getCurrentCFG()
    if not result['OK']:
        gLogger.error('Could not get current config from the CS')
        raise RuntimeError("Error finding current SEs.")

    for se, se_info in result['Value'].getAsDict('/Resources/StorageElements').iteritems():
        host = se_info.get('Host') or se_info.get('AccessProtocol.1', {}).get('Host')
        if host is not None:
            dirac_ses[se] = host
        else:
            gLogger.warn("No host found for SE: %s" % se)

    # Main loop
    ##############################
    for se, se_info in sorted(ses.iteritems()):
        if banned_ses is not None and se in banned_ses:
            gLogger.info("Skipping banned SE: %s" % se)
            continue

        try:
            se = SE(se=se,
                    se_info=se_info,
                    srms=srms,
                    xrootd_ports=get_xrootd_ports(se, host),
                    vo=vo,
                    vo_info=vo_info.get(se, {}),
                    existing_ses=dirac_ses)
        except Exception:
            gLogger.warn("Skipping problematic SE: %s" % se)
            continue
        se.write(cfg_system, '/Resources/StorageElements')
        host = se.Host or se.AccessProtocols[0].Host
        dirac_ses[se.DiracName] = host
    cfg_system.commit()


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


def update_ces(vo, domain='LCG', country_default='xx', host=None,
               banned_ces=None, max_processors=None):
    """
    Update the CEs in the Dirac config for certain VO.

    Args:
        vo (str): Updating CEs associated with this VO
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
    ##############################
    result = getBdiiCEInfo(vo, host=host)
    if not result['OK']:
        gLogger.error("Failed to call getBdiiCEInfo(vo=%s, host=%s): %s" % (vo, host, result['Message']))
        raise RuntimeError("getBdiiCEInfo failure.")

    ce_bdii_dict = result['Value']

    if not ce_bdii_dict:
        gLogger.warn("No CEs found in BDII")

    # Main update loop
    ##############################
    cfg_system = ConfigurationSystem()
    for site, site_info in sorted(ce_bdii_dict.iteritems()):  # pylint: disable=no-member
        try:
            s = Site(site, site_info, domain, country_default, banned_ces, max_processors)
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
    cfg_system.commit()

__all__ = ('update_ses', 'find_old_ses', 'update_ces', 'remove_old_ces')

if __name__ == '__main__':
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    update_ses(vo='gridpp', host='lcg-bdii.cern.ch:2170')
    update_ces(vo='gridpp', host='lcg-bdii.cern.ch:2170')
