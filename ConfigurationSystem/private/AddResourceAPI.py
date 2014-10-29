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
    if siteDict:
        gLogger.notice('New resources available:\n')
        for site, ces in siteDict.iteritems():
            diracSite = 'Unknown'
            result = getDIRACSiteName(site)
            if result['OK']:
                diracSite = ','.join(result['Value'])
            if ces:
                gLogger.notice("  %s, DIRAC site %s" % (site, diracSite))
                for ce, ce_info in ces.iteritems():
                    gLogger.notice(' '*4+ce)
                    gLogger.notice('      %s, %s'
                                   % (ce_info['CEType'],
                                      '%s_%s_%s' % ce_info['System']
                                      ))
    else:
        gLogger.notice('No new resources available, exiting')
        return S_OK()  # ceBdiiDict)  # (siteDict, ceBdiiDict))

    ## now we add them
    sitesAdded = []

    for site, ces in siteDict.iteritems():
        # Get the country code:
        country = ''
#    ces = siteDict[site].keys()
        for ce in ces:
            country = ce.strip().split('.')[-1].lower()
            if len(country) == 2:
                break
            if country == 'gov':
                country = 'us'
                break
        if not country or len(country) != 2:
            country = country_default
        result = getDIRACSiteName(site)
        if not result['OK']:
            #diracSite = "%s.%s.%s" % (domain, site, country)
            diracSite = diracSiteTemplate.format(domain=domain,
                                                 site=site,
                                                 country=country)
            gLogger.notice('The site %s is not yet in the CS, adding it as %s'
                           % (site, diracSite))
            diracSites = [diracSite]
        else:
            diracSites = result['Value']

        if len(diracSites) > 1:
            gLogger.notice('Attention! GOC site %s corresponds '
                           'to more than one DIRAC sites:' % site)
            gLogger.notice(str(diracSites))
            gLogger.notice('Please, pay attention which DIRAC '
                           'site the new CEs will join\n')

        newCEs = {}
        addedCEs = []
        for ce, ce_info in ces.iteritems():
            ceType = ce_info['CEType']
            for diracSite in diracSites:
                if ce in addedCEs:
                    continue
                gLogger.notice("Adding CE %s of type %s to %s"
                               % (ce, ceType, diracSite))
                newCEs.setdefault(diracSite, []).append(ce)
                addedCEs.append(ce)

        for diracSite in diracSites:
            if diracSite in newCEs:
                result = addSite(diracSite, site, ' '.join(newCEs[diracSite]))
                if not result['OK']:
                    gLogger.error('Error while executing addSite command',
                                  result['Message'])
                    if sitesAdded:
                        gLogger.notice('CEs were added at '
                                       'the following sites:')
                        for site, diracSite in sitesAdded:
                            gLogger.notice("%s\t%s" % (site, diracSite))
                    gLogger.error('Skipping site %s' % site)
                    continue

                sitesAdded.append((site, diracSite))
                result = updateSites(vo, ceBdiiDict)
                if not result['OK']:
                    gLogger.error('Problem updating the %s CE info in the CS'
                                  % diracSite, result['Message'])
                    continue
                gLogger.notice('Successfully updated %s CE info in CS' % diracSite)

    gLogger.notice('CEs were added at the following sites:')
    for site, diracSite in sitesAdded:
        gLogger.notice("%s\t%s" % (site, diracSite))
    return S_OK()  # ceBdiiDict)


## this comes from dirac-admin-add-site
def addSite(diracSiteName, gridSiteName, *ces):
    '''
    Add CEs for a particular dirac/grid site
    '''
    csAPI = CSAPI()

    try:
        diracGridType, _, _ = diracSiteName.split('.')
    except ValueError:
        gLogger.error("The DIRACSiteName should be of the form "
                      "GRID.LOCATION.COUNTRY for example LCG.CERN.ch")
        return S_ERROR('DIRACSiteName wrong format')

    result = getDIRACSiteName(gridSiteName)
    newSite = True
    if result['OK']:
        if result['Value']:
            if len(result['Value']) > 1:
                gLogger.notice('%s GOC site name is associated with several '
                               'DIRAC sites:' % gridSiteName)
                return S_ERROR('interactive input required to decide which '
                               'DIRAC site to use. Please use the command '
                               'line tool dirac-admin-add-site %s %s %s'
                               % (diracSiteName, gridSiteName, str(ces)))
            else:
                diracCSSite = result['Value'][0]
            if diracCSSite == diracSiteName:
                gLogger.notice('Site with GOC name %s is already '
                               'defined as %s' % (gridSiteName, diracSiteName))
                newSite = False
            else:
                gLogger.error('ERROR: Site with GOC name %s is already '
                              'defined as %s' % (gridSiteName, diracCSSite))
                return S_ERROR('site already defined')

    cfgBase = "/Resources/Sites/%s/%s" % (diracGridType, diracSiteName)
    change = False
    if newSite:
        gLogger.notice("Adding new site to CS: %s" % diracSiteName)
        csAPI.setOption("%s/Name" % cfgBase, gridSiteName)
        gLogger.notice("Adding CEs: %s" % ','.join(ces))
        csAPI.setOption("%s/CE" % cfgBase, ','.join(ces))
        change = True
    else:
        cesCS = set(gConfig.getValue("%s/CE" % cfgBase, []))
        ces = set(ces)
        newCEs = ces - cesCS
        if newCEs:
            cesCS = cesCS.union(ces)
            gLogger.notice("Adding CEs %s" % ','.join(newCEs))
            cesCS = cesCS.union(ces)
            csAPI.modifyValue("%s/CE" % cfgBase, ','.join(cesCS))
            change = True
    if change:
        res = csAPI.commitChanges()
        if not res['OK']:
            gLogger.error("Failed to commit changes to CS", res['Message'])
            return S_ERROR('Failed to commit changes to CS')
        else:
            if newSite:
                gLogger.notice("Successfully added site %s to the "
                               "CS with name %s and CEs: %s"
                               % (diracSiteName, gridSiteName, ','.join(ces)))
            else:
                gLogger.notice("Successfully added new CEs to site %s: %s"
                               % (diracSiteName, ','.join(newCEs)))
    return S_OK()


def updateSites(vo, ceBdiiDict):
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
    changeList = list(changeSet)
    changeList.sort()

    gLogger.notice('We are about to make the following changes to CS:\n')
    for entry in changeList:
        gLogger.notice("%s/%s %s -> %s" % entry)

    csAPI = CSAPI()
    csAPI.initialize()
    result = csAPI.downloadCSData()
    if not result['OK']:
        gLogger.error('Failed to initialize CSAPI object', result['Message'])
        return S_ERROR('Failed to initialize CSAPI object')
    for section, option, value, new_value in changeSet:
        if value == 'Unknown' or not value:
            csAPI.setOption(cfgPath(section, option), new_value)
        else:
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

    csAPI = CSAPI()
    csAPI.initialize()
    result = csAPI.downloadCSData()
    if not result['OK']:
        gLogger.error('Failed to initialize CSAPI object',
                      result['Message'])
        return S_ERROR('Failed to initialize CSAPI object')

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
            changeList = list(changeSet)
            changeList.sort()
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
            for section, option, value in changeList:
                csAPI.setOption(cfgPath(section, option), value)
    
            result = csAPI.commit()
            if not result['OK']:
                gLogger.error("Error while commit %s to CS"
                              % gridSE, result['Message'])
                gLogger.error("Skipping...")
                continue
            
            gLogger.notice("Successfully committed %d changes to CS"
                           % len(changeSet))
            result = updateSEs(vo)
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

    result = checkUnusedSEs(options.vo)
    if not result['OK']:
        gLogger.error("Error while running check for unused SEs",
                      result['Message'])
        sys.exit(1)

    #result = updateSEs(options.vo)
    #if not result['OK']:
    #    gLogger.error("Error while updating SEs", result['Message'])
    #    sys.exit(1)
