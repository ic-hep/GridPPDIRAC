"""
API for adding resources to CS
"""
from urlparse import urlparse
from AutoResourceTools.utils import get_se_vo_info, get_xrootd_ports
from AutoResourceTools.ConfigurationSystem import ConfigurationSystem
from AutoResourceTools.SETypes import SE
from AutoResourceTools.CETypes import Site
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.Grid import ldapSE, ldapService, getBdiiCEInfo


def update_ses(vo, host=None, banned_ses=None):
    """Update the SEs in the Dirac config for certain VO."""

    # Get SEs from DBII
    ##############################
    result = ldapSE('*', vo=vo, host=host)
    if not result['OK']:
        gLogger.error("Failed to call ldapSE('*', vo=%s, host=%s): %s" % (vo, host, result['Message']))
        raise RuntimeError("ldapSE failure.")

    ses = {se: se_info for se, se_info
           in ((i.get('GlueSEUniqueID', ''), i) for i in result['Value'])
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
            in ((urlparse(i.get('GlueServiceEndpoint', '')).hostname, i) for i in result['Value'])
            if hostname in ses}

    # Get dict of SE: VO info paths
    ##############################
    vo_info = get_se_vo_info(vo, host=host)
    if not vo_info:
        gLogger.warn("No SE -> VO info path mapping for any SE.")

    # Main loop
    ##############################
    dirac_ses = set()
    cfg_system = ConfigurationSystem()
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
        except:
            gLogger.warn("Skipping problematic SE: %s" % se)
            continue
        se.write(cfg_system, "/Resources/StorageElements")
        dirac_ses.add(se.DiracName)
    cfg_system.commit()


def update_ces(vo, domain='LCG', country_default='xx', host=None, banned_ces=None, max_processors=None):
    """Update the CEs in the Dirac config for certain VO."""

    # Get CE info from BDII
    ##############################
    result = getBdiiCEInfo(vo, host=host)
    if not result['OK']:
        gLogger.error("Failed to call getBdiiCEInfo(vo=%s, host=%s): %s" % (vo, host, result['Message']))
        raise RuntimeError("getBdiiCEInfo failure.")

    ce_bdii_dict = result['Value']

    if not ce_bdii_dict:
        gLogger.warn("No CEs found in BDII")

    # Main loop
    ##############################
    cfg_system = ConfigurationSystem()
    for site, site_info in sorted(ce_bdii_dict.iteritems()):
        try:
            s = Site(site, site_info, domain, country_default, banned_ces, max_processors)
        except:
            gLogger.warn("Skipping problematic site: %s" % site)
            continue
        s.write(cfg_system, cfgPath("/Resources/Sites", domain))
    cfg_system.commit()


if __name__ == '__main__':
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    update_ses(vo='gridpp', host='lcg-bdii.cern.ch:2170')
    update_ces(vo='gridpp', host='lcg-bdii.cern.ch:2170')
