# $HeadURL$
"""
API for adding resources to CS
"""
import os
import re
from urlparse import urlparse
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
from DIRAC.Core.Utilities.Grid import getBdiiCEInfo, getBdiiSEInfo, ldapSE, ldapService, ldapsearchBDII
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs


__all__ = ['checkUnusedCEs', 'checkUnusedSEs']


#VER_RE = re.compile(r"(?P<major_revision>[0-9])\.[0-9]+")


def _updateCS(changeSet):
    '''
    update CS
    '''
    csAPI = CSAPI()
    csAPI.initialize()
    result = csAPI.downloadCSData()
    if not result['OK']:
        gLogger.error('Failed to initialise CSAPI object', result['Message'])
        return S_ERROR('Failed to initialise CSAPI object')

    # remove cases where old [2] and new [3] values are the same
    # sort from set/generator/iterable into list
    changeList = sorted((i for i in changeSet if i[2] != i[3]))

    if not len(changeList):
        gLogger.notice('No changes required')
        return S_OK()

    gLogger.notice('Updating the CS...')
    gLogger.notice('------------------')
    gLogger.notice('We are about to make the following changes to CS:')

    for section, option, value, new_value in changeList:
        if value == new_value:
            continue
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
                   % len(changeList))
    return S_OK()


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

#def _map_os_ver(ce, os_name, os_version, os_release):
#    match = VER_RE.search(os_release)
#    if match:
#       return 'EL%s' % match.group('major_revision')
#    gLogger.warn("OS version information for ce '%s' cannot"
#                 "be determined from BDII" % ce)
#    return ' '.join((os_name, os_version, os_release)).strip()

def checkUnusedCEs(vo, host=None, domain='LCG', country_default='xx'):
    '''
    Check for unused CEs and add them where possible

    vo                - The VO
    domain            - The Grid domain used to generate
                        the DIRAC site name e.g. LCG
    country_default   - the default country code to use to substitute into
                        the dirac site name
    '''
    ## Get list of already known CEs from the CS
#    result = getCEsFromCS()
#    if not result['OK']:
#        gLogger.error('ERROR: failed to get CEs from CS', result['Message'])
#        return S_ERROR('failed to get CEs from CS')
#    knownCEs = result['Value']

    ## Now get from the BDII a list of ces that are not known i.e. new
#    ceBdiiDict = None
#    for host in alternative_bdii or []:
#        result = getBdiiCEInfo(vo, host)
#        if result['OK']:
#            ceBdiiDict = result['Value']
#            break
        
    #result = getGridCEs(vo, bdiiInfo=ceBdiiDict, ceBlackList=knownCEs)
    #if not result['OK']:
    #    gLogger.error('ERROR: failed to get CEs from BDII', result['Message'])
    #    return S_ERROR('failed to get CEs from BDII')
    #ceBdiiDict = result['BdiiInfo']
    result  = getBdiiCEInfo(vo, host=host)
    if not result['OK']:
        gLogger.error("Problem getting BDII info")
        return result
    ceBdiiDict = result['Value']

    ## now add the new resources
    cfgBase = "/Resources/Sites/%s" % domain
    changeSet = _configSet()
    for site, site_info in ceBdiiDict.iteritems():
        diracSite = '.'.join((domain, site))
        countryCodes = (ce.split('.')[-1].strip() for ce in site_info.get('CEs', {}).iterkeys())
        for countryCode in countryCodes:
            if countryCode == 'gov':
                diracSite = '.'.join((diracSite, 'us'))
                break
            if len(countryCode) == 2:
                diracSite = '.'.join((diracSite, countryCode))
                break
        else:
            diracSite = '.'.join((diracSite, country_default))
            
        if diracSite is None:
            gLogger.warn("Couldn't form a valid DIRAC name for site %s" % site)
            continue

        sitePath = cfgPath(cfgBase, diracSite)
        
        name = site_info.get('GlueSiteName').strip()
        description = site_info.get('GlueSiteDescription').strip()
        latitude = site_info.get('GlueSiteLatitude').strip()
        longitude = site_info.get('GlueSiteLongitude').strip()
        mail = site_info.get('GlueSiteSysAdminContact')\
                        .replace('mailto:', '')\
                        .strip()

                                
        ce_list = set()
        for ce, ce_info in site_info.get('CEs', {}).iteritems():
            ce_path = cfgPath(sitePath, 'CEs', ce)
            ce_list.add(ce)
            
            arch = ce_info.get('GlueHostArchitecturePlatformType', '')
            si00 = ce_info.get('GlueHostBenchmarkSI00', '')
            ram = ce_info.get('GlueHostMainMemoryRAMSize', '')
            os_name = ce_info.get('GlueHostOperatingSystemName', '')
            os_version = ce_info.get('GlueHostOperatingSystemVersion', '')
            os_release = ce_info.get('GlueHostOperatingSystemRelease', '')
                

            for queue, queue_info in ce_info.get('Queues', {}).iteritems():
                queue_path = cfgPath(ce_path, 'Queues', queue)
                
                ce_type = queue_info.get('GlueCEImplementationName', '')
                max_cpu_time = queue_info.get('GlueCEPolicyMaxCPUTime')
                vo = set((rule.replace('VO:','')
                          for rule in queue_info.get('GlueCEAccessControlBaseRule')
                          if rule.startswith('VO:')))
                q_si00 = ''
                capability = queue_info.get('GlueCECapability', [])
                if isinstance(capability, basestring):
                    capability = [capability]
                for i in capability:
                    if 'CPUScalingReferenceSI00' in i:
                        q_si00 = i.split('=')[-1].strip()
                        break
                    
                total_cpus = int(queue_info.get('GlueCEInfoTotalCPUs', 0))
                max_total_jobs =  min(1000, int(total_cpus/2))
                max_waiting_jobs =  max(2, int(max_total_jobs * 0.1))
                    
                changeSet.add(queue_path, 'VO', ', '.join(vo))
                changeSet.add(queue_path, 'SI00', q_si00)
                changeSet.add(queue_path, 'maxCPUTime', max_cpu_time)
                changeSet.add(queue_path, 'MaxTotalJobs', str(max_total_jobs))
                changeSet.add(queue_path, 'MaxWaitingJobs',
                              str(max_waiting_jobs))

            changeSet.add(ce_path, 'architecture', arch)
            changeSet.add(ce_path, 'SI00', si00)
            changeSet.add(ce_path, 'HostRAM', ram)
            changeSet.add(ce_path, 'CEType', ce_type)
            changeSet.add(ce_path, 'OS', 'EL%s'
                                         % os_release.split('.')[0].strip())
            if 'ARC' in ce_type or 'CREAM' in ce_type:
                changeSet.add(ce_path, 'SubmissionMode', 'Direct')

        changeSet.add(sitePath, 'Name', name)
        changeSet.add(sitePath, 'Description', description)
        changeSet.add(sitePath, 'Coordinates', '%s:%s' % (longitude, latitude))
        changeSet.add(sitePath, 'Mail', mail)
        changeSet.add(sitePath, 'CE', ', '.join(ce_list))
    return _updateCS(changeSet)

class SiteNamingDict(dict):
    def __init__(self, cfgBase):
        super(SiteNamingDict, self).__init__()
        result = gConfig.getSections(cfgBase)
        if not result['OK']:
            raise Exeption("Couldn't get current CS list of SEs")
        
        for s in result['Value']:
            r = gConfig.getOptionsDict(cfgPath(cfgBase, s, 'AccessProtocol.1'))
            if not r['OK'] or 'Host' not in r:
                r = gConfig.getOptionsDict(cfgPath(cfgBase, s))
                if not r['OK'] or 'Host' not in r:
                    continue
            self[r['Host']] = s
        
        
    def nextValidName(self, pattern):
        '''Return next valid DIRAC id from CN'''
        count = -1
        r = re.compile('%s(?P<se_index>[0-9]*?)-disk' % pattern)
        ## faster implementation than max
        for u in self.itervalues():
            match = r.match(u)
            if match:
                # or 0 catches the case with no numbers
                m = int(match.group('se_index') or 0)
                if m > count:
                    count = m
        if count == -1:
            return pattern + '-disk'
        return pattern + str(count + 1) + '-disk'
    

def _ldap_vo_info(vo_name, host=None):
    vo_filter = '(GlueVOInfoAccessControlBaseRule=VOMS:/%s/*)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlBaseRule=VOMS:/%s)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlBaseRule=VO:%s)' % vo_name
    filt = '(&(objectClass=GlueVOInfo)(|%s))' % vo_filter
    result = ldapsearchBDII(filt=filt)
    if not result['OK']:
        return result
    paths_mapping={}
    for se_info in result['Value']:
        if 'attr' not in se_info:
            continue
        if 'GlueChunkKey' not in se_info['attr']:
            continue
        for elem in se_info['attr']['GlueChunkKey']:
            if 'GlueSEUniqueID=' in elem:
                paths_mapping.setdefault(elem.replace('GlueSEUniqueID=',''),
                                         set())\
                             .add(se_info['attr']['GlueVOInfoPath'])

    ret = {}
    for se_name, vo_info_paths in paths_mapping.iteritems():
        sorted_paths = sorted(vo_info_paths, key=len)
        len_chk = len(set((len(path) for path in vo_info_paths)))\
                  != len(vo_info_paths)
        if len(vo_info_paths) > 1 and len_chk:
            gLogger.warn("There are multiple GlueVOInfoPath entries with the "
                         "same length for se: %s vo: %s, i.e. %s we will use "
                         "the first." % (se_name, vo_name, sorted_paths))
        norm_path = os.path.normpath(sorted_paths[0])
        basename = os.path.dirname(norm_path)
        ret[se_name] = {'Path': basename}
        if os.path.join(basename, vo_name) != norm_path:
            ret[se_name].update({'VOPath': norm_path})
    return S_OK(ret)

def checkUnusedSEs(vo, host=None):
    '''
    Check for unused SEs

    vo                - The VO
    host              - BDII host default, default = 'lcg-bdii.cern.ch:2170'
    '''
    
    result = ldapSE( '*', vo=vo, host=host)
    if not result['OK']:
      return result
    ses = dict(((i['GlueSEUniqueID'], i)  for i in result['Value']
                if 'GlueSEUniqueID' in i))
 
    result = ldapService( serviceType='SRM', vo=vo, host=host)
    if not result['OK']:
      return result
    srms = dict(((urlparse(i['GlueServiceEndpoint']).hostname, i)
                for i in result['Value'] if 'GlueServiceEndpoint' in i
                and urlparse(i['GlueServiceEndpoint']).hostname in ses))
    
    result = _ldap_vo_info(vo, host=host)
    if not result['OK']:
        return result
    vo_info = result['Value']
    
    changeSet = _configSet()
    cfgBase = '/Resources/StorageElements'
    mapping=SiteNamingDict(cfgBase)    
    for se, se_info in ses.iteritems():
        bdii_site_id = se_info.get('GlueSiteUniqueID')
        site = mapping.setdefault(se, mapping.nextValidName(bdii_site_id))

        seSection = cfgPath(cfgBase, site)
        accessSection = cfgPath(seSection, 'AccessProtocol.1')
        vopathSection = cfgPath(accessSection, 'VOPath')
        hostSection = seSection
        
        backend_type = se_info.get('GlueSEImplementationName', 'Unknown')
        description = se_info.get('GlueSEName')
        total_size = se_info.get('GlueSETotalOnlineSize', 'Unknown')
        base_rules = se_info.get('GlueSAAccessControlBaseRule', [])
        if not isinstance(base_rules, list):
            base_rules = [base_rules]
        bdiiVOs = set([re.sub('^VO:', '', rule) for rule in base_rules])
        
        srmDict = srms.get(se)
        if srmDict:
            hostSection = accessSection
            version = srmDict.get('GlueServiceVersion', '')
            if not version.startswith('2'):
                gLogger.warn("Not SRM version 2")
                continue
            
            url = urlparse(srmDict.get('GlueServiceEndpoint', ''))
            port = str(url.port)
            if port is None:
                gLogger.warn("No port determined for %s" % se)
                continue

            bdiiVOs = set([re.sub('^VO:', '', rule) for rule in
                           srmDict.get('GlueServiceAccessControlBaseRule', [])
                           ])            
            
            old_path = gConfig.getValue(cfgPath(accessSection, 'Path'), None)
            path = vo_info.get(se, {}).get('Path', '')
            vo_path = vo_info.get(se, {}).get('VOPath')
            
            # If path is different from last VO then we just default the
            # path to / and use the VOPath dict
            if old_path and path != old_path:
                vo_path = path
                path = '/'

            if vo_path:
                changeSet.add(vopathSection, vo, vo_path)
            

            changeSet.add(accessSection, 'Protocol', 'srm')
            changeSet.add(accessSection, 'ProtocolName', 'SRM2')
            changeSet.add(accessSection, 'Port', port)
            changeSet.add(accessSection, 'Access', 'remote')
            changeSet.add(accessSection, 'Path', path)
            changeSet.add(accessSection, 'SpaceToken', '')
            changeSet.add(accessSection, 'WSUrl', '/srm/managerv2?SFN=')

        changeSet.add(hostSection, 'Host', se)
        changeSet.add(seSection, 'BackendType', backend_type)
        changeSet.add(seSection, 'Description', description)
        changeSet.add(seSection, 'VO', ', '.join(bdiiVOs))
        changeSet.add(seSection, 'TotalSize', total_size)


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
    parser.add_option("-t", "--host", dest="host",
                      default='lcg-bdii.cern.ch:2170', metavar="HOST",
                      help="The LDAP host [default: %default]")

    (options, args) = parser.parse_args()

    gLogger.notice('-------------------------------------------------------')
    gLogger.notice('looking for new computing resources in BDII database...')
    gLogger.notice('-------------------------------------------------------')

    gLogger.notice('')
    gLogger.notice('** Checking for unused Sites/CEs')
    gLogger.notice('--------------------------------')

    result = checkUnusedCEs(options.vo,
                            host=options.host,
                            domain=options.domain)
    if not result['OK']:
        gLogger.error("Error while running check for unused CEs",
                      result['Message'])
        sys.exit(1)
    ceBdii = result['Value']

    gLogger.notice('')
    gLogger.notice('** Checking for unused Sites/SEs')
    gLogger.notice('--------------------------------')

    result = checkUnusedSEs(options.vo, host=options.host)
    if not result['OK']:
        gLogger.error("Error while running check for unused SEs:",
                      result['Message'])
        sys.exit(1)
    seBdii = result['Value']

#    gLogger.notice('')
#    gLogger.notice('-------------------------------------------------------')
#    gLogger.notice('Fetching updated info for sites in CS from BDII...     ')
#    gLogger.notice('-------------------------------------------------------')
#
#    gLogger.notice('')
#    gLogger.notice('** Checking for updates in CS defined Sites/CEs')
#    gLogger.notice('-----------------------------------------------')#
#
#    result = updateSites(options.vo, ceBdii)
#    if not result['OK']:
#        gLogger.error("Error while updating sites", result['Message'])
#        sys.exit(1)

