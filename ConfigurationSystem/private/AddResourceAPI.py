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

## Idea here is to allow it to properly exit if sites that have no new CEs are in list
#def _siteDictFilter(siteDict):
#    return {site: ces for site, ces in siteDict.iteritems() if not getDIRACSiteName(site)['OK'] or ces}


def checkUnusedCEs(vo, domain, country_default='xx',
                   diracSiteTemplate='{domain}.{site}.{country}'):
    '''
    Check for unused CEs and add them where possible

    vo                - The VO
    domain            - The Grid domain e.g. LCG
    country_default   - the default country code to use to substitute into
                        the diracSite name
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
    ## Helpful Note:
    #*************************************************************************
    #ces dict maps ce string to ce_info dict
    #---------------------------------------
    #example of ce string:
    #    'ceprod06.grid.hep.ph.ic.ac.uk'
    #example of the ce_info dict:
    #    {'System'  : ('CentOS', 'Final', '6.5'),
    #     'Queues'  : ['cream-sge-grid.q'],
    #     'GOCSite' : 'UKI-LT2-IC-HEP',
    #     'CEType'  : 'CREAM',
    #     'CEID'    : 'ceprod06.grid.hep.ph.ic.ac.uk'}
    #*************************************************************************

    ## Initialise the CSAPI
    csAPI = CSAPI()
    csAPI.initialize()
    result = csAPI.downloadCSData()
    if not result['OK']:
        gLogger.error('Failed to initialise CSAPI object', result['Message'])
        return S_ERROR('Failed to initialise CSAPI object')

    ## Get list of already known CEs from the CS
    result = getCEsFromCS()
    if not result['OK']:
        gLogger.error('ERROR: failed to get CEs from CS', result['Message'])
        return S_ERROR('failed to get CEs from CS')
    knownCEs = result['Value']

    ceBdiiDict = None
    gLogger.notice('looking for new computing resources '
                   'in the BDII database...')

    ## Now get from the BDII a list of ces that are not known i.e. new
    result = getGridCEs(vo, ceBlackList=knownCEs)
    if not result['OK']:
        gLogger.error('ERROR: failed to get CEs from BDII', result['Message'])
        return S_ERROR('failed to get CEs from BDII')
    ceBdiiDict = result['BdiiInfo']

    ## Check if there are actually any new resources to add
    siteDict = result.get('Value', {})
    if not siteDict:
        gLogger.notice('No new resources available, exiting')
        return S_OK()
    gLogger.notice('\nNew resources available:')
    gLogger.notice('------------------------')

    ## now add the new resources
    cfgBase = "/Resources/Sites/%s" % domain
    for site, ces in siteDict.iteritems():
        success_msg = ''
        diracSite = None
        result = getDIRACSiteName(site)
        if not result['OK']:  # DIRAC name not in CS, new site
            gLogger.notice("New site detected: %s" % site)
            country = _getCountryCode(ces.iterkeys(), country_default)
            diracSite = diracSiteTemplate.format(domain=domain,
                                                 site=site,
                                                 country=country)
            gLogger.notice('The site %s is not yet in the CS, adding it as %s'
                           % (site, diracSite))
            csAPI.setOption("%s/%s/Name" % (cfgBase, diracSite), site)
            success_msg = "Successfully added site %s to the "\
                          "CS with name %s\n" % (site, diracSite)
            if ces:
                gLogger.notice("New CE resources detected at %s(%s): %s"
                               % (site, diracSite, ','.join(ces)))
                gLogger.notice("Adding CE list: %s to %s(%s)"
                               % (','.join(ces), site, diracSite))
                csAPI.setOption("%s/%s/CE" % (cfgBase, diracSite),
                                ','.join(ces))
                success_msg = success_msg.replace('\n', ' and CE list %s\n'
                                                        % ','.join(ces))

        else:  # DIRAC name already in CS, existing site but new CE
            diracSites = result['Value']
            if len(diracSites) > 1:  # >1 DIRAC site for GOCBD site
                gLogger.notice('Attention! GOC site %s corresponds '
                               'to more than one DIRAC sites:' % site)
                gLogger.notice(str(diracSites))
                gLogger.notice('Interactive input required to decide which '
                               'DIRAC site to use. Please use the command '
                               'line tool dirac-admin-add-site DIRACSiteName '
                               '%s %s' % (site, str(ces.keys())))
                continue

            diracSite = diracSites[0]
            if ces:
                CSExistingCEs = set(gConfig.getValue("%s/%s/CE" % (cfgBase,
                                                                   diracSite),
                                                     []))
                gLogger.notice("New CE resources detected at %s(%s): %s"
                               % (site, diracSite, ','.join(ces)))
                gLogger.notice("Adding CEs %s to existing CE list for %s(%s)"
                               % (','.join(ces), site, diracSite))
                csAPI.modifyValue("%s/%s/CE" % (cfgBase, diracSite),
                                  ','.join(CSExistingCEs | set(ces)))  # Union
                success_msg = "Successfully added new CEs %s to existing"\
                              " CE list for site %s in CS\n"\
                              % (','.join(ces), diracSite)

        ## Commit changes for this site
        ## done site by site so that problems with 1 will not affect the rest
        result = csAPI.commitChanges()
        if not result['OK']:
            gLogger.error("Failed to commit changes to CS", result['Message'])
            gLogger.error("Skipping site: %s, DIRAC site: %s...\n"
                          % (site, diracSite))
            continue
        gLogger.notice(success_msg)

    ## Now that the sites and ce list are in CS,
    ## update their CS meta data from the BDII
    updateSites(vo, ceBdiiDict)
    return S_OK()


def updateSites(vo, ceBdiiDict=None):
    '''
    update sites
    '''
    gLogger.notice('Fetching updated information for sites in CS from BDII...')
    gLogger.notice('---------------------------------------------------------')
    result = getSiteUpdates(vo, bdiiInfo=ceBdiiDict)
    if not result['OK']:
        gLogger.error('Failed to get site updates', result['Message'])
        return S_ERROR('Failed to get site updates')
    changeSet = result['Value']
    return updateCS(changeSet)
#def _updateSites(vo, ceBdiiDict=None):
#    '''
#    update sites
#    '''
#    gLogger.notice('Fetching updated information for sites in CS from BDII...')
#    gLogger.notice('---------------------------------------------------------')
#    return getSiteUpdates(vo, bdiiInfo=ceBdiiDict)
#
#def updateSites(vo, ceBdiiDict=None):
#    result = _updateSites(vo, ceBdiiDict=None)
#    if not result['OK']:
#        gLogger.error('Failed to get site updates', result['Message'])
#        return S_ERROR('Failed to get site updates')
#    return updateCS(result['Value'])

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
    csAPI = CSAPI()
    csAPI.initialize()
    result = csAPI.downloadCSData()
    if not result['OK']:
        gLogger.error('Failed to initialize CSAPI object', result['Message'])
        return S_ERROR('Failed to initialize CSAPI object')

    changeList = list(changeSet)
    changeList.sort()

    gLogger.notice('\nUpdating the CS...')
    gLogger.notice('------------------')
    gLogger.notice('We are about to make the following changes to CS:')

    for section, option, value, new_value in changeSet:
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
    gLogger.notice("Successfully committed %d changes to CS" % len(changeSet))
    return S_OK()


def checkUnusedSEs(vo, diracSENameTemplate='{DIRACSiteName}-disk'):
    '''
    Check for unused SEs

    vo                  - The VO
    diracSENameTemplate - The template from which the DIRAC SE name is
                          generated.
                          Can use substitutions:
                              {domain}        - The Grid domain e.g. LCG,
                              {DIRACSiteName} - The DIRAC site name,
                              {country}       - The country code e.g. uk,
                              {gridSE}        - The Grid SE name
    '''
    
    ## Initialise the CSAPI
#    csAPI = CSAPI()
#    csAPI.initialize()
#    result = csAPI.downloadCSData()
#    if not result['OK']:
#        gLogger.error('Failed to initialise CSAPI object', result['Message'])
#        return S_ERROR('Failed to initialise CSAPI object')
    
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

#    changeSetFull = set()
    changeSet = configSet()

#    csAPI = CSAPI()
#    csAPI.initialize()
#    result = csAPI.downloadCSData()
#    if not result['OK']:
#        gLogger.error('Failed to initialize CSAPI object',
#                      result['Message'])
#        return S_ERROR('Failed to initialize CSAPI object')

    for site, ses in siteSRMDict.iteritems():
        for gridSE, se_info in ses.iteritems():
            #changeSet = set()
            seDict = se_info['SE']
            srmDict = se_info['SRM']
            # Check the SRM version
            version = srmDict.get('GlueServiceVersion', '')
            if not (version and version.startswith('2')):
                gLogger.debug('Skipping SRM service with version %s' % version)
                continue
            result = getDIRACSiteName(site)
            if not result['OK']:
                gLogger.notice('Unused se %s is detected at unused site %s'
                               % (gridSE, site))
                gLogger.notice('Consider adding site %s to the DIRAC CS'
                               % site)
                continue
            diracSites = result['Value']
            ## here
            if len(diracSites) > 1:
                gLogger.notice('Can not determine to which DIRAC site the '
                               'new SE should be attached:')
                gLogger.notice(str(diracSites.values()))
                gLogger.notice('Please run the dirac-admin-add-resources '
                               'command line tool to decide interactively')
                continue
            diracSite = diracSites[0]

            ## here
            domain, siteName, country = diracSite.split('.')
            diracSEName = diracSENameTemplate.format(domain=domain,
                                                     DIRACSiteName=siteName,
                                                     country=country,
                                                     gridSE=gridSE)
            gLogger.notice('Grid SE %s will get the DIRAC name %s'
                           % (gridSE, diracSEName))

            gLogger.notice('Adding new SE %s at site %s'
                           % (diracSEName, diracSite))
            cfgBase = '/Resources/StorageElements'
            seSection = cfgPath(cfgBase, diracSEName)
            #csAPI.setOption("%s/BackendType" % seSection,
            #                seDict.get('GlueSEImplementationName', 'Unknown'))
            #csAPI.setOption("%s/Description" % seSection,
            #                seDict.get('GlueSEName', 'Unknown'))
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
            #csAPI.setOption("%s/VO" % seSection,
            #                ','.join(seVOs))
            changeSet.add(seSection, 'VO', ','.join(seVOs))
            accessSection = cfgPath(seSection, 'AccessProtocol.1')
            #csAPI.setOption(cfgPath(accessSection, 'Protocol'),
            #                'srm')
            #csAPI.setOption(cfgPath(accessSection, 'ProtocolName'),
            #                'SRM2')
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
            #csAPI.setOption(cfgPath(accessSection, 'Host'), host)
            #csAPI.setOption(cfgPath(accessSection, 'Port'), port)
            #csAPI.setOption(cfgPath(accessSection, 'Access'), 'remote')
            changeSet.add(accessSection, 'Host', host)
            changeSet.add(accessSection, 'Port', port)
            changeSet.add(accessSection, 'Access', 'remote')
            # Try to guess the Path
            domain = '.'.join(host.split('.')[-2:])
            path = '/dpm/%s/home' % domain
            
            #csAPI.setOption(cfgPath(accessSection, 'Path'), path)
            #csAPI.setOption(cfgPath(accessSection, 'SpaceToken'), '')
            #csAPI.setOption(cfgPath(accessSection, 'WSUrl'), '/srm/managerv2?SFN=')
            changeSet.add(accessSection, 'Path', path)
            changeSet.add(accessSection, 'SpaceToken', '')
            changeSet.add(accessSection, 'WSUrl', '/srm/managerv2?SFN=')

            #gLogger.notice('SE %s will be added with the following parameters')
            #changeList = list(changeSet)
            #changeList.sort()
            #for entry in changeList:
            #    gLogger.notice(entry)
            #changeSetFull = changeSetFull.union(changeSet)

            #csAPI = CSAPI()
            #csAPI.initialize()
            #result = csAPI.downloadCSData()
            #if not result['OK']:
            #    gLogger.error('Failed to initialize CSAPI object',
            #                  result['Message'])
            #    return S_ERROR('Failed to initialize CSAPI object')
            #changeList = list(changeSetFull)
            #changeList.sort()
            #for section, option, value in changeList:
            #    csAPI.setOption(cfgPath(section, option), value)

            #result = csAPI.commit()
            #if not result['OK']:
            #    gLogger.error("Error while commit %s to CS"
            #                  % gridSE, result['Message'])
            #    gLogger.error("Skipping...")
            #    continue
            #result = updateCS(changeSet)
#            result = csAPI.commitChanges()
#            if not result['OK']:
#                gLogger.error("Failed to commit changes to CS", result['Message'])
#                gLogger.error("Skipping gridSE: %s ..." % gridSE)
#                #gLogger.error("Skipping site: %s, DIRAC site: %s...\n"
#                #          % (site, diracSite))
#                continue

#            if not result['OK']:
#                gLogger.error('Failed to update the CS for %s SE, Skipping...' % gridSE)
#                continue
#
#            gLogger.notice("Successfully committed %d changes to CS"
#                           % len(changeSet))
#            result = updateSEs(vo)
            #if not result['OK']:
            #    gLogger.error('Failed to update %s SE info in CS' % gridSE)
            #    continue
            gLogger.notice('Successfully updated %s SE info in CS\n' % gridSE)
            
    ## duplicate from updateSE - only works once they have already been updated
    result = getSRMUpdates(vo)
    if not result['OK']:
        gLogger.error('Failed to get SRM updates', result['Message'])
        return S_ERROR('Failed to get SRM updates')
    changeSet.update(result['Value'])
    
    return updateCS(changeSet)
#    updateSEs(vo)
#    return S_OK()


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
    print options.vo

    result = checkUnusedCEs(options.vo, options.domain)
    if not result['OK']:
        gLogger.error("Error while running check for unused CEs",
                      result['Message'])
        sys.exit(1)

    #result = updateSites(options.vo, result['Value'])
    #if not result['OK']:
    #    gLogger.error("Error while updating sites", result['Message'])
    #    sys.exit(1)

    result = checkUnusedSEs(options.vo)
    if not result['OK']:
        gLogger.error("Error while running check for unused SEs:",
                      result['Message'])
        sys.exit(1)

    result = updateSEs(options.vo)
    if not result['OK']:
        gLogger.error("Error while updating SEs:", result['Message'])
        sys.exit(1)
