# pylint: disable=invalid-name
"""Auto SE BDII to config tools."""
import os
import re
import copy
from itertools import chain
from collections import Counter
from datetime import date
from urlparse import urlparse

import ldap
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from GridPPDIRAC.ConfigurationSystem.private.AutoResourceTools.ConfigurationSystem import ConfigurationSystem

VO_REGEX = re.compile(r'^VO:\s*(?P<voname>[\w.-]+)')

LATENCY_MAPPING = {'online': 'disk',
                   'nearline': 'tape'}


def ldapsearch_bdii_ses(address=('lcg-bdii.cern.ch', 2170),
                        base='Mds-Vo-name=local,o=grid',
                        scope=ldap.SCOPE_SUBTREE,
                        latency_mapping=None,
                        cfg_base_path='/Resources/StorageElements'):
    """Return processes SE information from BDII."""
    if latency_mapping is None:
        latency_mapping = LATENCY_MAPPING

    # Open LDAP connection to BDII
    ldap_conn = ldap.open(*address)

    # Get SA records
    # ##############
    # using wildcard enforces that attribute MUST be present
    sas = ldap_conn.search_s(base=base, scope=scope,
                             filterstr="(&(objectClass=GlueSA)"
                             "(GlueChunkKey=*))")
    sa_dict = {}
    for _, sa in sorted(sas):
        se = max(sa['GlueChunkKey'], key=len).replace('GlueSEUniqueID=', '')
        if 'GlueSAAccessLatency' in sa:
            latency = latency_mapping.get(max(sa['GlueSAAccessLatency'], key=len).lower(),
                                          'disk')
        else:
            latency = 'disk'
        sa_dict.setdefault(se, {}).setdefault(latency, []).append(sa)

    # Get SRM records
    # ###############
    srms = ldap_conn.search_s(base=base, scope=scope,
                              filterstr="(&(GlueServiceType=SRM)"
                              "(GlueServiceEndpoint=*)"
                              "(GlueForeignKey=*)"
                              "(GlueServiceVersion=2*))")
    srm_dict = {}
    for key, srm in sorted(srms):
        if 'Mds-Vo-name=%s' % max(srm['GlueForeignKey'], key=len).replace('GlueSiteUniqueID=', '')\
           not in key:
            continue
        se = urlparse(max(srm['GlueServiceEndpoint'], key=len)).hostname
        if se in srm_dict:
            gLogger.warn("SE '%s' already in SRM dict so will not be added again" % se)
            continue
        srm_dict[se] = srm

    # Get Existing Config SEs
    # #######################
    cs = ConfigurationSystem()
    result = cs.getCurrentCFG()
    if not result['OK']:
        gLogger.error('Could not get current config from the CS')
        raise RuntimeError("Error finding current SEs.")
    existing_dirac_names = {}
    all_dirac_se_names = set()  # convienience for later
    # tape and disk might share se?
    for se, se_info in sorted(result['Value'].getAsDict(cfg_base_path).iteritems()):
        # str is the case for Options, e.g. DefaultProtocols
        if not isinstance(se_info, dict) or 'Host' not in se_info:
            continue
        all_dirac_se_names.add(se)
        host = se_info['Host']
        latency = se.rsplit('-', 1)[-1]
        latency_dict = existing_dirac_names.setdefault(host, {})
        if latency in latency_dict:
            gLogger.warn("Host '%s-%s' already in existing_dirac_names wont updte it"
                         % (host, latency))
            continue

        latency_dict[latency] = {'dirac_name': se}
        for key, value in se_info.iteritems():
            if key.startswith('AccessProtocol.') and 'Protocol' in value:
                latency_dict[latency][value['Protocol']] = int(key.rsplit('.', 1)[-1])

    # Get SE records
    # ##############
    ses = ldap_conn.search_s(base=base, scope=scope,
                             filterstr="(&(objectClass=GlueSE)(GlueSEUniqueID=*))")
    se_dict = {}
    dirac_name_counter = Counter()
    for key, se in sorted(ses):
        bdii_name = max(se['GlueForeignKey'], key=len).replace('GlueSiteUniqueID=', '')
        if 'Mds-Vo-name=%s' % bdii_name not in key:
            continue

        # attach DIRAC name and VOs
        host = max(se['GlueSEUniqueID'], key=len)
        se['host'] = host
        srm_vos = sa_dict.get(host, {}).get('GlueSAAccessControlBaseRule', [])
        latency_dict = sa_dict.get(host, {})
        for latency, sas in latency_dict.iteritems():
            current_dirac_cfg = existing_dirac_names.get(host, {}).get(latency, {})
            dirac_name = current_dirac_cfg.get('dirac_name')
            if not dirac_name:
                name_root = '%s-%s' % (bdii_name, latency)
                count_number_regex = re.compile(r'%s(?P<count>\d*)-%s' % (bdii_name, latency))
                number = [int(count_number_regex.match(i).group('count') or 0)
                          for i in all_dirac_se_names
                          if count_number_regex.match(i)]
                if number:
                    dirac_name_counter[name_root] = max(number) + 1
                dirac_name = '%s%s-%s' % (bdii_name, dirac_name_counter[name_root] or '', latency)
                dirac_name_counter[name_root] += 1

            se['dirac_name'] = dirac_name
            srm_ap_index = current_dirac_cfg.get('srm')
            xroot_ap_index = current_dirac_cfg.get('root')
            if srm_ap_index:
                se['srm_ap_index'] = srm_ap_index
            if xroot_ap_index:
                se['xroot_ap_index'] = xroot_ap_index
            se['vos'] = set(VO_REGEX.match(rule).group('voname') for rule in
                            (srm_vos or
                             chain.from_iterable(sa.get('GlueSAAccessControlBaseRule', [])
                                                 for sa in sas))
                            if VO_REGEX.match(rule))
            if dirac_name in se_dict:
                gLogger.warn("DIRAC name '%s' already in dict, won't add it again" % dirac_name)
                continue
            # We copy the SE dict here otherwise a later entry (i.e. a -tape latency)
            # will update the earlier (-disk?) entry causing things such as the VO
            # list to be incorrectly overwritten.
            se_dict[dirac_name] = copy.deepcopy(se)

    # Get XRootD ports
    # ################
    xrootports = ldap_conn.search_s(base=base, scope=scope,
                                    filterstr="(&(objectClass=GlueSEAccessProtocol)"
                                    "(GlueChunkKey=*)"
                                    "(GlueSEAccessProtocolEndpoint=*)"
                                    "(GlueSEAccessProtocolType=Root))")
    xrootport_dict = {}
    for _, xrootport in xrootports:
        # this loop and one in voinfo could be condensed to urlparse(endpoint).hostname probably
        for key in xrootport['GlueChunkKey']:
            if 'GlueSEUniqueID=' in key:
                se = key.replace('GlueSEUniqueID=', '')
                break
        port = urlparse(xrootport.get('GlueSEAccessProtocolEndpoint', '')[0]).port
        if port is not None:
            xrootport_dict.setdefault(se, set()).add(port)

    # Get VO info records
    # ###################
    voinfos = ldap_conn.search_s(base=base, scope=scope,
                                 filterstr="(&(objectClass=GlueVOInfo)"
                                 "(GlueChunkKey=*)"
                                 "(GlueVOInfoAccessControlBaseRule=*)"
                                 "(GlueVOInfoPath=*))")
    voinfo_dict = {}
    for _, voinfo in voinfos:
        for key in voinfo['GlueChunkKey']:
            if 'GlueSEUniqueID=' in key:
                se = key.replace('GlueSEUniqueID=', '')
                break

        vo = voinfo['GlueVOInfoAccessControlBaseRule'][0]
        if vo.startswith('VO:'):
            voinfo_dict.setdefault(se, {})\
                       .setdefault(vo.replace('VO:', ''), set())\
                       .add(voinfo['GlueVOInfoPath'][0])
    for se, vo_info in voinfo_dict.iteritems():
        vo_info['common_path'] = os.path.dirname(
            os.path.commonprefix([i if i.endswith(os.sep) else i + os.sep
                                  for i in chain.from_iterable(vo_info.values())]))
    return se_dict, sa_dict, srm_dict, xrootport_dict, voinfo_dict


def update_ses(considered_vos=None, cfg_base_path='/Resources/StorageElements',
               address=('lcg-bdii.cern.ch', 2170), banned_ses=None):
    """Update the list of Storage Elements in DIRAC config."""
    se_dict, _, srm_dict, xrootport_dict, vopaths_dict\
        = ldapsearch_bdii_ses(address=address,
                              cfg_base_path=cfg_base_path)

    cs = ConfigurationSystem()
    for se, se_info in sorted(se_dict.iteritems()):
        if banned_ses is not None and se in banned_ses:
            gLogger.info("Skipping banned SE: %s" % se)
            continue
        site_path = os.path.join(cfg_base_path, se)
        vos = se_info.get('vos', set())
        # only consider certain vos.
        if considered_vos is not None and not vos.intersection(considered_vos):
            continue

        host = se_info['host']
        cs.add(site_path, 'BackendType', max(se_info['GlueSEImplementationName']))
        cs.add(site_path, 'Description', max(se_info.get('GlueSEName', [None])))
        cs.add(site_path, 'Host', host)
        cs.add(site_path, 'LastSeen', date.today().strftime('%d/%m/%Y'))
        cs.add(site_path, 'TotalSize', max(se_info.get('GlueSETotalOnlineSize', ['Unknown'])))
        cs.add(site_path, 'VO', vos)

        # Get access protocols
        srm_ap_index = se_info.get('srm_ap_index')
        xroot_ap_index = se_info.get('xroot_ap_index')
        srm = srm_dict.get(host, {})
        xrootdports = xrootport_dict.get(host, set())
        vopaths = vopaths_dict.get(host, {})
        common_path = vopaths.pop('common_path', '')
        if srm and vopaths:
            if srm_ap_index is None:
                srm_ap_index = 1
                if xroot_ap_index is not None:
                    srm_ap_index = xroot_ap_index + 1
            ap_path = os.path.join(site_path, 'AccessProtocol.%s' % srm_ap_index)
            port = urlparse(srm.get('GlueServiceEndpoint', [''])[0]).port
            cs.add(ap_path, 'Access', 'remote')
            cs.add(ap_path, 'Host', host)
            cs.add(ap_path, 'Path', common_path)
            cs.add(ap_path, 'PluginName', 'GFAL2_SRM2')
            cs.add(ap_path, 'Port', port)
            cs.add(ap_path, 'Protocol', 'srm')
            cs.add(ap_path, 'SpaceToken', '')
            cs.add(ap_path, 'WSUrl', '/srm/managerv2?SFN=')
            vo_path = os.path.join(ap_path, 'VOPath')
            for vo_name, paths in sorted(vopaths.iteritems()):
                valid_paths = sorted(path for path in paths if not path.isupper())
                if valid_paths:
                    cs.add(vo_path, vo_name, min(valid_paths, key=len))

        if xrootdports and vopaths:
            if xroot_ap_index is None:
                xroot_ap_index = 1
                if srm_ap_index is not None:
                    xroot_ap_index = srm_ap_index + 1
            ap_path = os.path.join(site_path, 'AccessProtocol.%s' % xroot_ap_index)
            cs.add(ap_path, 'Access', 'remote')
            cs.add(ap_path, 'Host', host)
            cs.add(ap_path, 'Path', common_path)
            cs.add(ap_path, 'PluginName', 'GFAL2_XROOT')
            cs.add(ap_path, 'Port', 1094 if 1094 in xrootdports else min(xrootdports))
            cs.add(ap_path, 'Protocol', 'root')
            cs.add(ap_path, 'SpaceToken', '')
            vo_path = os.path.join(ap_path, 'VOPath')
            for vo_name, paths in sorted(vopaths.iteritems()):
                valid_paths = sorted(path for path in paths if not path.isupper())
                if valid_paths:
                    cs.add(vo_path, vo_name, min(valid_paths, key=len))

    cs.commit()


if __name__ == '__main__':
    Script.parseCommandLine(ignoreErrors=True)
    update_ses()
