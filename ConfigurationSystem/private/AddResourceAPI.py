# $HeadURL$
"""
API for adding resources to CS
"""
import re
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Utilities import (getGridCEs,
                                                        getSiteUpdates,
                                                        getCEsFromCS,
                                                        getGridSRMs,
                                                        getSRMUpdates
                                                        )
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.Pfn import pfnparse
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs


__all__ = ['checkUnusedCEs', 'checkUnusedSEs', 'updateSites', 'updateSEs']


def _updateCS(changeSet):
    '''
    update CS
    '''
    if not len(changeSet):
        gLogger.notice('No changes required')
        return S_OK()

    csAPI = CSAPI()
    csAPI.initialize()
    result = csAPI.downloadCSData()
    if not result['OK']:
        gLogger.error('Failed to initialise CSAPI object', result['Message'])
        return S_ERROR('Failed to initialise CSAPI object')

    changeList = list(changeSet)
    changeList.sort()

    gLogger.notice('Updating the CS...')
    gLogger.notice('------------------')
    gLogger.notice('We are about to make the following changes to CS:')

    for section, option, value, new_value in changeList:
        if value == 'Unknown' or not value:
            gLogger.notice("Setting %s/%s:   -> %s"
                           % (section, option, new_value))
            csAPI.setOption(cfgPath(section, option), new_value)
        else:
            gLogger.notice("Modifying %s/%s:   %s -> %s"
                           % (section, option, value, new_value))
            csAPI.modifyValue(cfgPath(section, option), new_value)

    result = csAPI.commit()
    if not result['OK']:
        gLogger.error("Error while commit to CS", result['Message'])
        return S_ERROR("Error while commit to CS")
    gLogger.notice("Successfully committed %d changes to CS\n"
                   % len(changeSet))
    return S_OK()


def _getCountryCode(hosts, default):
    '''
    Given a list of hosts try to determine the country code
    '''
    for countryCode in (h.strip().split('.')[-1].lower() for h in hosts):
        if countryCode == 'gov':
            return 'us'
        if len(countryCode) == 2:
            return countryCode
    return default


def _getUpdateDiracSiteName(site, domain, diracSiteTemplate,
                            iterable, country_default, changeSet):
    '''
    Return the DIRAC site name for a given site if it is known about in the CS.
    If it is a new site then add it to the CS changeSet and return then new
    DIRAC name built from the templates and country_default/host list iterable
    '''
    result = getDIRACSiteName(site)
    if result['OK'] and len(result['Value']) > 1:  # >1 DIRAC name for site
        gLogger.notice('Attention! GOC site %s corresponds '
                       'to more than one DIRAC sites:' % site)
        gLogger.notice(str(result['Value']))
        gLogger.notice('Interactive input required to decide which '
                       'DIRAC site to use. Please run the '
                       'dirac-admin-add-resources '
                       'command line tool to decide interactively')
        return None, changeSet
    elif result['OK']:  # DIRAC name already in CS, existing site but new CE
        diracSite = result['Value'][0]
    else:  # DIRAC name not in CS, new site
        gLogger.notice("New site detected: %s" % site)
        country = _getCountryCode(iterable, country_default)
        diracSite = diracSiteTemplate.format(domain=domain,
                                             site=site,
                                             country=country)
        gLogger.notice('The site %s is not yet in the CS, adding it as %s'
                       % (site, diracSite))
        changeSet.add("/Resources/Sites/%s/%s"
                      % (domain, diracSite), 'Name', site)
    return diracSite, changeSet


class _configSet(set):
    '''
    Wrapper class around set to provide a nicer add syntax
    and also to get the element in the form expected for _updateCS
    '''
    def add(self, section, option, new_value):
        '''
        Overrides base class add giving nicer syntax for our needs
        '''
        super(_configSet, self).add((section,
                                    option,
                                    gConfig.getValue(cfgPath(section, option),
                                                     None),
                                    new_value))


def updateSites(vo, ceBdiiDict=None):
    '''
    update sites
    '''
    result = getSiteUpdates(vo, bdiiInfo=ceBdiiDict)
    if not result['OK']:
        gLogger.error('Failed to get site updates', result['Message'])
        return S_ERROR('Failed to get site updates')
    changeSet = result['Value']
    return _updateCS(changeSet)


def updateSEs(vo):
    '''
    update SEs
    '''
    result = getSRMUpdates(vo)
    if not result['OK']:
        gLogger.error('Failed to get SRM updates', result['Message'])
        return S_ERROR('Failed to get SRM updates')
    changeSet = result['Value']
    return _updateCS(changeSet)


def checkUnusedCEs(vo, domain='LCG', country_default='xx',
                   diracSiteTemplate='{domain}.{site}.{country}'):
    '''
    Check for unused CEs and add them where possible

    vo                - The VO
    domain            - The Grid domain used to generate
                        the DIRAC site name e.g. LCG
    country_default   - the default country code to use to substitute into
                        the dirac site name
    diracSiteTemplate - The template from which the DIRAC site name is
                        generated:
                        Can use substitutions:
                              {domain}        - The Grid domain e.g. LCG,
                              {site}          - The site name
                              {country}       - The country code e.g. uk,
                                                defaulting to country_default
                                                if it cannot be determined
                                                automatically
    '''
    ## Get list of already known CEs from the CS
    result = getCEsFromCS()
    if not result['OK']:
        gLogger.error('ERROR: failed to get CEs from CS', result['Message'])
        return S_ERROR('failed to get CEs from CS')
    knownCEs = result['Value']

    ceBdiiDict = None

    ## Now get from the BDII a list of ces that are not known i.e. new
    result = getGridCEs(vo, ceBlackList=knownCEs)
    if not result['OK']:
        gLogger.error('ERROR: failed to get CEs from BDII', result['Message'])
        return S_ERROR('failed to get CEs from BDII')
    ceBdiiDict = result['BdiiInfo']

    ## Check if there are actually any new resources to add
    siteDict = result.get('Value', {})
    if not siteDict:
        gLogger.notice('No new CE resources available')
        return S_OK()

    ## now add the new resources
    cfgBase = "/Resources/Sites/%s" % domain
    changeSet = _configSet()
    for site, ces in siteDict.iteritems():
        diracSite, changeSet = _getUpdateDiracSiteName(site,
                                                       domain,
                                                       diracSiteTemplate,
                                                       ces.iterkeys(),
                                                       country_default,
                                                       changeSet)
        if diracSite is None:
            continue
        sitePath = cfgPath(cfgBase, diracSite)
        if ces:
            CSExistingCEs = set(gConfig.getValue("%s/CE" % sitePath, []))
            gLogger.notice("New CE resource(s) detected at %s(%s): %s\n"
                           % (site, diracSite, ','.join(ces)))
            changeSet.add(sitePath, 'CE', ','.join(CSExistingCEs | set(ces)))

    result = _updateCS(changeSet)
    result['Value'] = ceBdiiDict
    return result


def checkUnusedSEs(vo, domain='LCG', country_default='xx',
                   diracSiteTemplate='{domain}.{site}.{country}',
                   diracSENameTemplate='{DIRACSiteName}-disk'):
    '''
    Check for unused SEs

    vo                  - The VO
    domain            - The Grid domain used to generate
                        the DIRAC site name e.g. LCG
    country_default   - the default country code to use to substitute into
                        the dirac site name
    diracSiteTemplate - The template from which the DIRAC site name is
                        generated:
                        Can use substitutions:
                              {domain}        - The Grid domain e.g. LCG,
                              {site}          - The site name
                              {country}       - The country code e.g. uk,
                                                defaulting to country_default
                                                if it cannot be determined
                                                automatically
    diracSENameTemplate - The template from which the DIRAC SE name is
                          generated.
                          Can use substitutions:
                              {domain}        - The Grid domain e.g. LCG,
                              {DIRACSiteName} - The DIRAC site name,
                              {country}       - The country code e.g. uk,
                              {gridSE}        - The Grid SE name
    '''
    result = getGridSRMs(vo, unUsed=True)
    if not result['OK']:
        gLogger.error('Failed to look up SRMs in BDII', result['Message'])
        return S_ERROR('Failed to look up SRMs in BDII')
    siteSRMDict = result.get('Value', {})

    # Evaluate VOs
    result = getVOs()
    if result['OK']:
        csVOs = set(result['Value'])
    else:
        csVOs = set([vo])

    changeSet = _configSet()
    cfgBase = '/Resources/StorageElements'
    for site, ses in siteSRMDict.iteritems():
        diracSite, changeSet = _getUpdateDiracSiteName(site,
                                                       domain,
                                                       diracSiteTemplate,
                                                       ses.iterkeys(),
                                                       country_default,
                                                       changeSet)
        if diracSite is None:
            continue

        for se, se_info in ses.iteritems():
            seDict = se_info['SE']
            srmDict = se_info['SRM']

            # Check the SRM version
            version = srmDict.get('GlueServiceVersion', '')
            if not (version and version.startswith('2')):
                gLogger.debug('Skipping SRM service with version %s' % version)
                continue

            result = pfnparse(srmDict.get('GlueServiceEndpoint', ''))
            if not result['OK']:
                gLogger.error('Can not get the SRM service end point. '
                              'Skipping ...')
                continue

            host = result['Value']['Host']
            port = result['Value']['Port']
            # Try to guess the Path
            path = '/dpm/%s/home' % '.'.join(host.split('.')[-2:])
            bdiiVOs = set([re.sub('^VO:', '', rule) for rule in
                           srmDict.get('GlueServiceAccessControlBaseRule', [])
                           ])
            seVOs = csVOs.intersection(bdiiVOs)
            backend_type = seDict.get('GlueSEImplementationName', 'Unknown')
            description = seDict.get('GlueSEName', 'Unknown')

            siteDomain, siteName, siteCountry = diracSite.split('.')
            diracSEName = diracSENameTemplate.format(domain=siteDomain,
                                                     DIRACSiteName=siteName,
                                                     country=siteCountry,
                                                     girdSE=se)
            gLogger.notice('Adding new SE %s with DIRAC name %s at site %s'
                           % (se, diracSEName, diracSite))

            # Create the CS paths
            seSection = cfgPath(cfgBase, diracSEName)
            accessSection = cfgPath(seSection, 'AccessProtocol.1')

            # Add the changes
            changeSet.add(seSection, 'BackendType', backend_type)
            changeSet.add(seSection, 'Description', description)
            changeSet.add(seSection, 'VO', ','.join(seVOs))
            changeSet.add(accessSection, 'Protocol', 'srm')
            changeSet.add(accessSection, 'ProtocolName', 'SRM2')
            changeSet.add(accessSection, 'Host', host)
            changeSet.add(accessSection, 'Port', port)
            changeSet.add(accessSection, 'Access', 'remote')
            changeSet.add(accessSection, 'Path', path)
            changeSet.add(accessSection, 'SpaceToken', '')
            changeSet.add(accessSection, 'WSUrl', '/srm/managerv2?SFN=')

            gLogger.notice('Successfully updated %s SE info in CS\n' % se)

    return _updateCS(changeSet)


if __name__ == '__main__':
    import sys
    from optparse import OptionParser
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    parser = OptionParser()
    parser.add_option("-v", "--vo", dest="vo",
                      default='gridpp', metavar="VO",
                      help="The VO [default: %default]")
    parser.add_option("-d", "--domain", dest="domain",
                      default='LCG', metavar="DOMAIN",
                      help="The Grid domain e.g. [default: %default]")

    (options, args) = parser.parse_args()

    gLogger.notice('-------------------------------------------------------')
    gLogger.notice('looking for new computing resources in BDII database...')
    gLogger.notice('-------------------------------------------------------')

    gLogger.notice('')
    gLogger.notice('** Checking for unused Sites/CEs')
    gLogger.notice('--------------------------------')

    result = checkUnusedCEs(options.vo, options.domain)
    if not result['OK']:
        gLogger.error("Error while running check for unused CEs",
                      result['Message'])
        sys.exit(1)
    ceBdii = result['Value']

    gLogger.notice('')
    gLogger.notice('** Checking for unused Sites/SEs')
    gLogger.notice('--------------------------------')

    result = checkUnusedSEs(options.vo, options.domain)
    if not result['OK']:
        gLogger.error("Error while running check for unused SEs:",
                      result['Message'])
        sys.exit(1)

    gLogger.notice('')
    gLogger.notice('-------------------------------------------------------')
    gLogger.notice('Fetching updated info for sites in CS from BDII...     ')
    gLogger.notice('-------------------------------------------------------')

    gLogger.notice('')
    gLogger.notice('** Checking for updates in CS defined Sites/CEs')
    gLogger.notice('-----------------------------------------------')

    result = updateSites(options.vo, ceBdii)
    if not result['OK']:
        gLogger.error("Error while updating sites", result['Message'])
        sys.exit(1)

    gLogger.notice('')
    gLogger.notice('** Checking for updates in CS defined SEs')
    gLogger.notice('-----------------------------------------')

    result = updateSEs(options.vo)
    if not result['OK']:
        gLogger.error("Error while updating SEs:", result['Message'])
        sys.exit(1)
