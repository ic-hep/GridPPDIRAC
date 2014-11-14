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
import code

def getCountryCode(ces, default):
    for countryCode in (ce.strip().split('.')[-1].lower() for ce in ces):
        if countryCode == 'gov':
            return 'us'
        if len(countryCode)==2:
            return countryCode
    return default

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
    *************************************************************************
    ces dict maps ce string to ce_info dict
    ---------------------------------------
    example of ce string:
        'ceprod06.grid.hep.ph.ic.ac.uk'
    example of the ce_info dict:
        {'System'  : ('CentOS', 'Final', '6.5'),
         'Queues'  : ['cream-sge-grid.q'],
         'GOCSite' : 'UKI-LT2-IC-HEP',
         'CEType'  : 'CREAM',
         'CEID'    : 'ceprod06.grid.hep.ph.ic.ac.uk'}
    *************************************************************************
    '''
    ceBdiiDict = None
    gLogger.notice('looking for new computing resources '
                   'in the BDII database...')

    result = getCEsFromCS()
    if not result['OK']:
        gLogger.error('ERROR: failed to get CEs from CS', result['Message'])
        return S_ERROR('failed to get CEs from CS')
    knownCEs = result['Value']

    result = getGridCEs(vo, ceBlackList=knownCEs)
    if not result['OK']:
        gLogger.error('ERROR: failed to get CEs from BDII', result['Message'])
        return S_ERROR('failed to get CEs from BDII')
    ceBdiiDict = result['BdiiInfo']

    siteDict = result.get('Value', {})
    if not siteDict:
        gLogger.notice('No new resources available, exiting')
        return S_OK()  # ceBdiiDict)  # (siteDict, ceBdiiDict))
    gLogger.notice('New resources available:\n')
        
    csAPI = CSAPI()
    csAPI.initialize()
    result = csAPI.downloadCSData()
    if not result['OK']:
        gLogger.error('Failed to initialize CSAPI object', result['Message'])
        return S_ERROR('Failed to initialize CSAPI object')
    ## now we add them
    #sitesAdded = []    
    for site, ces in siteDict.iteritems():
        
        #country = country_default
        #for ce, ce_info in ces.iteritems():
            #gLogger.notice("New CE resource detected at site %s:   %s, %s, %s" % (site, ce, ce_info['CEType'], '%s_%s_%s' % ce_info['System']))
            # Get the country code:
            #country = set((i.strip().split('.')[-1].lower() for i in ces if len(i.strip().split('.')[-1].lower())==2))
            #if country == country_default:
            #    c_tmp = ce.strip().split('.')[-1].lower()
            #    if len(c_tmp) == 2:
            #        country = c_tmp
            #    if c_tmp == 'gov':
            #        country = 'us'
#    ces = siteDict[site].keys()

        cfgBase = "/Resources/Sites/%s" % domain
        result = getDIRACSiteName(site)
        diracSite = None
        success_msg = ''
        if not result['OK']:  # DIRAC name not in CS, new site
            gLogger.notice("New site detected: %s" % site)
            country = getCountryCode(ces.iterkeys(), country_default)
            #diracSite = "%s.%s.%s" % (domain, site, country)
            diracSite = diracSiteTemplate.format(domain=domain,
                                                 site=site,
                                                 country=country)
            cfgBase += '/%s' % diracSite
            gLogger.notice('The site %s is not yet in the CS, adding it as %s'
                           % (site, diracSite))
            csAPI.setOption("%s/Name" % cfgBase, site)
            if ces:
                #for ce, ce_info in ces.iteritems():
                #    gLogger.notice("New CE resource detected at site %s:   %s, %s, %s" % (site, ce, ce_info['CEType'], '%s_%s_%s' % ce_info['System']))
                gLogger.notice("New CE resources detected at site(DIRAC site) %s(%s): %s" % (site, diracSite, ','.join(ces)))
                gLogger.notice("Adding CE list: %s to site(DIRAC site): %s(%s)" % (','.join(ces), site, diracSite))
                csAPI.setOption("%s/CE" % cfgBase, ','.join(ces))
            #result = csAPI.commitChanges()
            #if not result['OK']:
            #    gLogger.error("Failed to commit changes to CS", result['Message'])
            #    gLogger.error("Skipping site: %s, DIRAC site: %s..." % (site, diracSite))
            #    continue
            #gLogger.notice("Successfully added site %s to the "
            #               "CS with name %s and CEs: %s"
            #               % (diracSite, site, ','.join(ces)))
            success_msg = "Successfully added site %s to the "\
                           "CS with name %s and CE list: %s\n"\
                           % (site, diracSite, ','.join(ces))

        else:  # DIRAC name already in CS, existing site but new CE
            diracSites = result['Value']
            if len(diracSites) > 1:
                gLogger.notice('Attention! GOC site %s corresponds '
                               'to more than one DIRAC sites:' % site)
                gLogger.notice(str(diracSites))
                gLogger.notice('Interactive input required to decide which '
                               'DIRAC site to use. Please use the command '
                               'line tool dirac-admin-add-site DIRACSiteName %s %s'
                               % (site, str(ces.keys())))
                continue
            
            diracSite = diracSites[0]
            cfgBase += '/%s' % diracSite
            if ces:
                CSExistingCEs = set(gConfig.getValue("%s/CE" % cfgBase, []))
                #newCEs = set(ces) - CSExistingCEs  # This should == set(ces) as we filter only unknown ces
                gLogger.notice("New CE resources detected at site(DIRAC site) %s(%s): %s" % (site, diracSite, ','.join(ces)))
                gLogger.notice("Adding CEs %s to existing CE list for site(DIRAC site) %s(%s)" % (','.join(ces), site, diracSite))#newCEs))
                csAPI.modifyValue("%s/CE" % cfgBase, ','.join(CSExistingCEs | set(ces)))  # Union
                
                #res = csAPI.commitChanges()
                #if not res['OK']:
                #    gLogger.error("Failed to commit changes to CS", res['Message'])
                #    gLogger.error("Skipping site: %s, DIRAC site: %s..." % (site, diracSite))
                #    continue
                #gLogger.notice("Successfully added new CEs to site %s: %s"
                #           % (diracSite, ','.join(ces)))
                success_msg = "Successfully added in CS new CEs to site %s: %s\n"\
                              % (diracSite, ','.join(ces))

        result = csAPI.commitChanges()
        if not result['OK']:
            gLogger.error("Failed to commit changes to CS", result['Message'])
            gLogger.error("Skipping site: %s, DIRAC site: %s...\n" % (site, diracSite))
            continue
        gLogger.notice(success_msg)
        #sitesAdded.append((site, diracSite))

#    gLogger.notice('CEs were added at the following sites:')
#    for site, diracSite in sitesAdded:
#        gLogger.notice("%s\t%s" % (site, diracSite))
    updateSites(vo, ceBdiiDict)
    return S_OK()  # ceBdiiDict)


def updateSites(vo, ceBdiiDict=None):
    '''
    update sites
    '''
    gLogger.notice('Fetching updated information for sites in CS from BDII...')
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
    csAPI = CSAPI()
    csAPI.initialize()
    result = csAPI.downloadCSData()
    if not result['OK']:
        gLogger.error('Failed to initialize CSAPI object', result['Message'])
        return S_ERROR('Failed to initialize CSAPI object') 
    
    changeList = list(changeSet)
    changeList.sort()

    gLogger.notice('Updating the CS...')
    gLogger.notice('We are about to make the following changes to CS:\n')

    for section, option, value, new_value in changeSet:
        if value == 'Unknown' or not value:
            gLogger.notice("Setting %s/%s:   -> %s" % (section, option, new_value))
            csAPI.setOption(cfgPath(section, option), new_value)
        else:
            gLogger.notice("Modifying %s/%s:   %s -> %s" % (section, option, value, new_value))
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
    result = getGridSRMs(vo, unUsed=True)
    if not result['OK']:
        gLogger.error('Failed to look up SRMs in BDII', result['Message'])
        return S_ERROR('Failed to look up SRMs in BDII')
    siteSRMDict = result['Value']

    # Evaluate VOs
    result = getVOs()
    if result['OK']:
        csVOs = set(result['Value'])
    else:
        csVOs = set([vo])

    #changeSetFull = set()

#    csAPI = CSAPI()
#    csAPI.initialize()
#    result = csAPI.downloadCSData()
#    if not result['OK']:
#        gLogger.error('Failed to initialize CSAPI object',
#                      result['Message'])
#        return S_ERROR('Failed to initialize CSAPI object')

    for site, ses in siteSRMDict.iteritems():
        for gridSE, se_info in ses.iteritems():
            changeSet = set()
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
            seSection = cfgPath('/Resources/StorageElements', diracSEName)
            changeSet.add((seSection, 'BackendType',
                           seDict.get('GlueSEImplementationName', 'Unknown')))
            changeSet.add((seSection, 'Description',
                           seDict.get('GlueSEName', 'Unknown')))
            bdiiVOs = set([re.sub('^VO:', '', rule) for rule in
                           srmDict.get('GlueServiceAccessControlBaseRule',
                                       []
                                       )
                           ])
            seVOs = csVOs.intersection(bdiiVOs)
            changeSet.add((seSection, 'VO', ','.join(seVOs)))
            accessSection = cfgPath(seSection, 'AccessProtocol.1')
            changeSet.add((accessSection, 'Protocol', 'srm'))
            changeSet.add((accessSection, 'ProtocolName', 'SRM2'))
            endPoint = srmDict.get('GlueServiceEndpoint', '')
            result = pfnparse(endPoint)
            if not result['OK']:
                gLogger.error('Can not get the SRM service end point. '
                              'Skipping ...')
                continue
            host = result['Value']['Host']
            port = result['Value']['Port']
            changeSet.add((accessSection, 'Host', host))
            changeSet.add((accessSection, 'Port', port))
            changeSet.add((accessSection, 'Access', 'remote'))
            # Try to guess the Path
            domain = '.'.join(host.split('.')[-2:])
            path = '/dpm/%s/home' % domain
            changeSet.add((accessSection, 'Path', path))
            changeSet.add((accessSection, 'SpaceToken', ''))
            changeSet.add((accessSection, 'WSUrl', '/srm/managerv2?SFN='))

            gLogger.notice('SE %s will be added with the following parameters')
            #changeList = list(changeSet)
            #changeList.sort()
            for entry in changeList:
                gLogger.notice(entry)
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
            result = updateCS(changeSet)
#            if not result['OK']:
#                gLogger.error('Failed to update the CS for %s SE, Skipping...' % gridSE)
#                continue
#            
#            gLogger.notice("Successfully committed %d changes to CS"
#                           % len(changeSet))
#            result = updateSEs(vo)
            if not result['OK']:
                gLogger.error('Failed to update %s SE info in CS' % gridSE)
                continue
            gLogger.notice('Successfully updated %s SE info in CS' % gridSE)

    return S_OK()


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

#    result = checkUnusedSEs(options.vo)
#    if not result['OK']:
#        gLogger.error("Error while running check for unused SEs",
#                      result['Message'])
#        sys.exit(1)

    #result = updateSEs(options.vo)
    #if not result['OK']:
    #    gLogger.error("Error while updating SEs", result['Message'])
    #    sys.exit(1)
