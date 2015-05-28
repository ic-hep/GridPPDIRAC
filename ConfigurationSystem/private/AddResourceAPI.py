# $HeadURL$
"""
API for adding resources to CS
"""
import os
import re
from datetime import datetime, date, timedelta
from types import GeneratorType
from urlparse import urlparse
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
#from DIRAC.ConfigurationSystem.Client.Utilities import (getGridCEs,
#                                                        getSiteUpdates,
#                                                        getCEsFromCS,
#                                                        getGridSRMs,
#                                                        getSRMUpdates
#                                                        )
#from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
#from DIRAC.Core.Utilities.Pfn import pfnparse
#from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs
from DIRAC.Core.Utilities.Grid import (getBdiiCEInfo, getBdiiSEInfo, ldapSE,
                                       ldapService, ldapsearchBDII)


__all__ = ['checkUnusedCEs', 'checkUnusedSEs', 'removeOldCEs']


#VER_RE = re.compile(r"(?P<major_revision>[0-9])\.[0-9]+")



class _ConfigurationSystem(CSAPI):
    """ Class to smartly wrap the functionality of the CS"""

    def __init__(self):
        """initialise"""
        CSAPI.__init__(self)
        self._num_changes = 0
        result = self.initialize()
        if not result['OK']:
            gLogger.error('Failed to initialise CSAPI object', result['Message'])
            raise RuntimeError(result['Message'])   

    def add(self, section, option, new_value):
        """
        Add a value into the configuration system.

        This method will overwrite any existing option's value.

        Args:
            section (str): The section
            option (str): The option to be created/modified
            new_value: The value to be assigned

        Example:
            >>> _ConfigurationSystem().add('/Registry', 'DefaultGroup', 'dteam_user')
        """
        if isinstance(new_value, (tuple, list, set, GeneratorType)):
            new_value = ', '.join(sorted(map(str, new_value)))
        else:
            new_value = str(new_value)

        old_value = gConfig.getValue(cfgPath(section, option), None)
        if old_value == new_value:
            return

        if old_value is None:
            gLogger.notice("Setting %s/%s:   -> %s"
                           % (section, option, new_value))
            self.setOption(cfgPath(section, option), new_value)
        else:
            gLogger.notice("Modifying %s/%s:   %s -> %s"
                           % (section, option, old_value, new_value))
            self.modifyValue(cfgPath(section, option), new_value)
        self._num_changes+=1

    def append_unique(self, section, option, new_value):
        """
        Append a value onto the end of an existing CS option.

        This method is like append except that it ensures that the final list
        of values for the given option only contains unique entries.
        """
        old_values = set(v.strip() for v in gConfig.getValue(cfgPath(section, option), '').split(',') if v)

        if isinstance(new_value, (tuple, list, set)):
            old_values.update(map(str, new_value))
        else:
            old_values.add(str(new_value))
        self.add(section, option, old_values)

    def append(self, section, option, new_value):
        """
        Append a value onto the end of an existing CS option.

        This method is like add with the exception that the new value
        is appended on to the end of the list of values associated
        with that option.
        """
        old_values = [v.strip() for v in gConfig.getValue(cfgPath(section, option), '').split(',') if v]

        if isinstance(new_value, (tuple, list, set)):
            old_values.extend(new_value)
        else:
            old_values.append(new_value)
        self.add(section, option, old_values)
            
    def remove(self, section, option=None):
        """
        Remove a section/option from the configuration system.

        This method will remove the specified section if the option argument
        is None (default). If the option argument is given then that option
        (formed of section/option) is removed.

        Args:
            section (str): The section
            option (str): [optional] The option

        Example:
            >>> _ConfigurationSystem().remove('/Registry', 'DefaultGroup')
        """
        if option is None:
            gLogger.notice("Removing section %s" % section)
            self.delSection(section)
        else:
            gLogger.notice("Removing option %s/%s" % (section, option))
            self.delOption(cfgPath(section, option))
        self._num_changes+=1

    def commit(self):
        """
        Commit the changes to the configuration system.

        Returns:
            dict: S_OK/S_ERROR DIRAC style dicts
        """
        result = CSAPI.commit(self)
        if not result['OK']:
            gLogger.error("Error while commit to CS", result['Message'])
            return S_ERROR("Error while commit to CS")
        if self._num_changes:
            gLogger.notice("Successfully committed %d changes to CS\n"
                           % self._num_changes)
            self._num_changes = 0
            return S_OK()
        gLogger.notice("No changes to commit")
        return S_OK()

def removeOldCEs(threshold=5, domain='LCG'):
    '''
    Remove CEs that have not been seen for a given time
    '''
    cs = _ConfigurationSystem()
    result = cs.getCurrentCFG()
    if not result['OK']:
        gLogger.error('Could not get current sites from the CS')
        return result
    base_path = cfgPath('/Resources/Sites', domain)
    site_dict = result['Value'].getAsDict(base_path)
    for site, site_info in site_dict.iteritems():
        site_path = cfgPath(base_path, site)
        for ce, ce_info in site_info.get('CEs', {}).iteritems():
            ce_path = cfgPath(site_path, 'CEs', ce)
            if 'LastSeen' not in ce_info:
                gLogger.debug("No LastSeen info for CE: %s at site: %s" % (ce, site))
                continue
            last_seen = datetime.strptime(ce_info['LastSeen'], '%d/%m/%Y').date()
            if date.today() - last_seen > timedelta(days=threshold):
                cs.remove(section=ce_path)
    return cs.commit()

def checkUnusedCEs(vo, host=None, domain='LCG', country_default='xx'):
    '''
    Check for unused CEs and add them where possible

    vo                - The VO
    domain            - The Grid domain used to generate
                        the DIRAC site name e.g. LCG
    country_default   - the default country code to use to substitute into
                        the dirac site name
    '''
    result = getBdiiCEInfo(vo, host=host)
    if not result['OK']:
        gLogger.error("Problem getting BDII info")
        return result
    ceBdiiDict = result['Value']

    ## now add the new resources
    cfgBase = "/Resources/Sites/%s" % domain
    changeSet = _ConfigurationSystem()
    for site, site_info in sorted(ceBdiiDict.iteritems()):
        diracSite = '.'.join((domain, site))
        countryCodes = (ce.split('.')[-1].strip()
                        for ce in site_info.get('CEs', {}).iterkeys())
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
        for ce, ce_info in sorted(site_info.get('CEs', {}).iteritems()):
            ce_path = cfgPath(sitePath, 'CEs', ce)
            ce_list.add(ce)

            arch = ce_info.get('GlueHostArchitecturePlatformType', '')
            si00 = ce_info.get('GlueHostBenchmarkSI00', '')
            ram = ce_info.get('GlueHostMainMemoryRAMSize', '')
            os_name = ce_info.get('GlueHostOperatingSystemName', '')
            os_version = ce_info.get('GlueHostOperatingSystemVersion', '')
            os_release = ce_info.get('GlueHostOperatingSystemRelease', '')

            for queue, queue_info in sorted(ce_info.get('Queues', {}).iteritems()):
                queue_path = cfgPath(ce_path, 'Queues', queue)

                ce_type = queue_info.get('GlueCEImplementationName', '')
                max_cpu_time = queue_info.get('GlueCEPolicyMaxCPUTime')
                acbr = queue_info.get('GlueCEAccessControlBaseRule')
                vos = set((rule.replace('VO:', '') for rule in acbr
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
                max_total_jobs = min(1000, int(total_cpus/2))
                max_waiting_jobs = max(2, int(max_total_jobs * 0.1))

                changeSet.append_unique(queue_path, 'VO', vos)
                changeSet.add(queue_path, 'SI00', q_si00)
                changeSet.add(queue_path, 'maxCPUTime', max_cpu_time)
                changeSet.add(queue_path, 'MaxTotalJobs', max_total_jobs)
                changeSet.add(queue_path, 'MaxWaitingJobs', max_waiting_jobs)

            # The CEType needs to be "ARC" but the BDII contains "ARC-CE"
            if ce_type == 'ARC-CE':
              ce_type = 'ARC'

            changeSet.add(ce_path, 'LastSeen', date.today().strftime('%d/%m/%Y'))
            changeSet.add(ce_path, 'architecture', arch)
            changeSet.add(ce_path, 'SI00', si00)
            changeSet.add(ce_path, 'HostRAM', ram)
            changeSet.add(ce_path, 'CEType', ce_type)
            changeSet.add(ce_path, 'OS', 'EL%s'
                                         % os_release.split('.')[0].strip())
            if 'ARC' in ce_type:
                changeSet.add(ce_path, 'SubmissionMode', 'Direct')
                changeSet.add(ce_path, 'JobListFile', '%s-jobs.xml' % ce)
            elif 'CREAM' in ce_type:
                changeSet.add(ce_path, 'SubmissionMode', 'Direct')

        changeSet.add(sitePath, 'Name', name)
        changeSet.add(sitePath, 'Description', description)
        changeSet.add(sitePath, 'Coordinates', '%s:%s' % (longitude, latitude))
        changeSet.add(sitePath, 'Mail', mail)
        changeSet.append_unique(sitePath, 'CE', ce_list)
    return changeSet.commit()


class SiteNamingDict(dict):
    '''Dict for site names'''
    def __init__(self, cfgBase):
        super(SiteNamingDict, self).__init__()
        result = gConfig.getSections(cfgBase)
        if not result['OK']:
            raise Exception("Couldn't get current CS list of SEs")

        for s in result['Value']:
            r = gConfig.getOptionsDict(cfgPath(cfgBase, s, 'AccessProtocol.1'))
            if not r['OK'] or 'Host' not in r['Value']:
                r = gConfig.getOptionsDict(cfgPath(cfgBase, s))
                if not r['OK'] or 'Host' not in r['Value']:
                    continue
            self[r['Value']['Host']] = s

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
    '''function for getting VO SE path info'''
    vo_filter = '(GlueVOInfoAccessControlBaseRule=VOMS:/%s/*)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlBaseRule=VOMS:/%s)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlBaseRule=VO:%s)' % vo_name
    filt = '(&(objectClass=GlueVOInfo)(|%s))' % vo_filter
    result = ldapsearchBDII(filt=filt, host=host)
    if not result['OK']:
        return result

    paths_mapping = {}
    for se_info in result['Value']:
        if 'attr' not in se_info:
            continue
        if 'GlueChunkKey' not in se_info['attr']:
            continue
        for elem in se_info['attr']['GlueChunkKey']:
            if 'GlueSEUniqueID=' in elem:
                paths_mapping.setdefault(elem.replace('GlueSEUniqueID=', ''),
                                         set())\
                             .add(se_info['attr']['GlueVOInfoPath'])

    ret = {}
    for se_name, vo_info_paths in paths_mapping.iteritems():
        sorted_paths = sorted(vo_info_paths, key=len)
        len_orig = len(vo_info_paths)
        len_unique = len(set((len(path) for path in vo_info_paths)))
        if len_orig > 1 and len_unique != len_orig:
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
    result = ldapSE('*', vo=vo, host=host)
    if not result['OK']:
        return result
    ses = dict(((i['GlueSEUniqueID'], i) for i in result['Value']
                if 'GlueSEUniqueID' in i))

    result = ldapService(serviceType='SRM', vo=vo, host=host)
    if not result['OK']:
        return result
    srms = dict(((urlparse(i['GlueServiceEndpoint']).hostname, i)
                for i in result['Value'] if 'GlueServiceEndpoint' in i
                and urlparse(i['GlueServiceEndpoint']).hostname in ses))

    result = _ldap_vo_info(vo, host=host)
    if not result['OK']:
        return result
    vo_info = result['Value']

    changeSet = _ConfigurationSystem()
    cfgBase = '/Resources/StorageElements'
    mapping = SiteNamingDict(cfgBase)
    for se, se_info in sorted(ses.iteritems()):
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

            ## DIRACs Bdii2CSAgent used the ServiceAccessControlBaseRule value
            bdiiVOs = set([re.sub('^VO:', '', rule) for rule in
                           srmDict.get('GlueServiceAccessControlBaseRule', [])
                           ])

            old_path = gConfig.getValue(cfgPath(accessSection, 'Path'), None)
            path = vo_info.get(se, {}).get('Path')
            vo_path = vo_info.get(se, {}).get('VOPath')

            # If path is different from last VO then we just default the
            # path to / and use the VOPath dict
            if old_path and path and path != old_path:
                vo_path = vo_path or os.path.join(path, vo)
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
        changeSet.add(seSection, 'VO', bdiiVOs)
        changeSet.add(seSection, 'TotalSize', total_size)

    return changeSet.commit()

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

    gLogger.notice('')
    gLogger.notice('** Checking for old sites')
    gLogger.notice('-------------------------')

    result = removeOldCEs(domain=options.domain)
    if not result['OK']:
        gLogger.error("Error while running check for old sites:",
                      result['Message'])
        sys.exit(1)

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
