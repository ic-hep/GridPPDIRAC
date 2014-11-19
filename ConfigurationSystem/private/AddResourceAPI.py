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
#from DIRAC.Core.Utilities.Grid import ldapService, getBdiiSEInfo
from DIRAC.Core.Utilities.Pfn import pfnparse
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs
#import code

class configSet(set):
    def add(self, section, option, new_value):
        super(configSet, self).add((section,
                                    option,
                                    gConfig.getValue(cfgPath(section, option), None),
                                    new_value))

## Idea here is to allow it to properly exit if sites that have no new CEs are in list
#def _siteDictFilter(siteDict):
#    return {site: ces for site, ces in siteDict.iteritems() if not getDIRACSiteName(site)['OK'] or ces}


def updateSites(vo, ceBdiiDict=None):
    '''
    update sites
    '''
    result = getSiteUpdates(vo, bdiiInfo=ceBdiiDict)
    if not result['OK']:
        gLogger.error('Failed to get site updates', result['Message'])
        return S_ERROR('Failed to get site updates')
    changeSet = result['Value']
    return updateCS(changeSet)

def updateSEs(vo):
    '''
    update SEs
    '''
    result = getSRMUpdates(vo)
    if not result['OK']:
        gLogger.error('Failed to get SRM updates', result['Message'])
        return S_ERROR('Failed to get SRM updates')
    changeSet = result['Value']
    return updateCS(changeSet)


def updateCS(changeSet):
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
        gLogger.error('Failed to initialize CSAPI object', result['Message'])
        return S_ERROR('Failed to initialize CSAPI object')

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
    gLogger.notice("Successfully committed %d changes to CS\n" % len(changeSet))
    return S_OK()

def _getCountryCode(ces, default):
    '''
    Given a list of CEs try to determine the country code
    '''
    for countryCode in (ce.strip().split('.')[-1].lower() for ce in ces):
        if countryCode == 'gov':
            return 'us'
        if len(countryCode) == 2:
            return countryCode
    return default

def _getUpdateDiracSiteName(site, domain, diracSiteTemplate, iterable, country_default, changeSet):
    result = getDIRACSiteName(site)
    if result['OK'] and len(result['Value']) > 1:  # >1 DIRAC site for GOCBD site
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
        changeSet.add("/Resources/Sites/%s/%s" % (domain, diracSite), 'Name', site)
    return diracSite, changeSet

def checkUnusedCEs(vo, domain, country_default='xx',
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
    changeSet = configSet()
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

    result = updateCS(changeSet)
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

    changeSet = configSet()
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

            siteDomain, siteName, siteCountry = diracSite.split('.')
            diracSEName = diracSENameTemplate.format(domain=siteDomain,
                                                     DIRACSiteName=siteName,
                                                     country=siteCountry,
                                                     girdSE=se)
            gLogger.notice('Grid SE %s will get the DIRAC name %s'
                           % (se, diracSEName))

            gLogger.notice('Adding new SE %s at site %s'
                           % (diracSEName, diracSite))
            
            seSection = cfgPath(cfgBase, diracSEName)

            changeSet.add(seSection, 'BackendType',
                           seDict.get('GlueSEImplementationName', 'Unknown'))
            changeSet.add(seSection, 'Description',
                           seDict.get('GlueSEName', 'Unknown'))
            bdiiVOs = set([re.sub('^VO:', '', rule) for rule in
                           srmDict.get('GlueServiceAccessControlBaseRule',
                                       []
                                       )
                           ])
            seVOs = csVOs.intersection(bdiiVOs)

            changeSet.add(seSection, 'VO', ','.join(seVOs))
            accessSection = cfgPath(seSection, 'AccessProtocol.1')

            changeSet.add(accessSection, 'Protocol', 'srm')
            changeSet.add(accessSection, 'ProtocolName', 'SRM2')
            endPoint = srmDict.get('GlueServiceEndpoint', '')
            result = pfnparse(endPoint)
            if not result['OK']:
                gLogger.error('Can not get the SRM service end point. '
                              'Skipping ...')
                continue
            host = result['Value']['Host']
            port = result['Value']['Port']

            changeSet.add(accessSection, 'Host', host)
            changeSet.add(accessSection, 'Port', port)
            changeSet.add(accessSection, 'Access', 'remote')
            # Try to guess the Path
            path = '/dpm/%s/home' % '.'.join(host.split('.')[-2:])

            changeSet.add(accessSection, 'Path', path)
            changeSet.add(accessSection, 'SpaceToken', '')
            changeSet.add(accessSection, 'WSUrl', '/srm/managerv2?SFN=')

            gLogger.notice('Successfully updated %s SE info in CS\n' % se)
   
    return updateCS(changeSet)


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

    gLogger.notice('-----------------------------------------------------------')
    gLogger.notice('looking for new computing resources in the BDII database...')
    gLogger.notice('-----------------------------------------------------------')
    
    #gLogger.notice('-----------------------------------------------------------')
    gLogger.notice('')
    gLogger.notice('** Checking for unused Sites/CEs')
    gLogger.notice('--------------------------------')
    #gLogger.notice('-----------------------------------------------------------')
    
    result = checkUnusedCEs(options.vo, options.domain)
    if not result['OK']:
        gLogger.error("Error while running check for unused CEs",
                      result['Message'])
        sys.exit(1)
    ceBdii = result['Value']

    #gLogger.notice('-----------------------------------------------------------')
    gLogger.notice('')
    gLogger.notice('** Checking for unused Sites/SEs')
    gLogger.notice('--------------------------------')
    #gLogger.notice('-----------------------------------------------------------')

    result = checkUnusedSEs(options.vo, options.domain)
    if not result['OK']:
        gLogger.error("Error while running check for unused SEs:",
                      result['Message'])
        sys.exit(1)
    
    gLogger.notice('')
    gLogger.notice('-----------------------------------------------------------')
    gLogger.notice('Fetching updated information for sites in CS from BDII...  ')
    gLogger.notice('-----------------------------------------------------------')
    
    #gLogger.notice('-----------------------------------------------------------')
    gLogger.notice('')
    gLogger.notice('** Checking for updates in CS defined Sites/CEs')
    gLogger.notice('-----------------------------------------------')
    #gLogger.notice('-----------------------------------------------------------')

    result = updateSites(options.vo, ceBdii)
    if not result['OK']:
        gLogger.error("Error while updating sites", result['Message'])
        sys.exit(1)
    
    #gLogger.notice('-----------------------------------------------------------')
    gLogger.notice('')
    gLogger.notice('** Checking for updates in CS defined SEs')
    gLogger.notice('-----------------------------------------')
    #gLogger.notice('-----------------------------------------------------------')

    result = updateSEs(options.vo)
    if not result['OK']:
        gLogger.error("Error while updating SEs:", result['Message'])
        sys.exit(1)
