"""Dirac multi VO Storage element types."""
import os
import re
from datetime import date
from collections import namedtuple
from urlparse import urlparse
from DIRAC import gLogger
from .utils import WritableMixin  # , splitcommonvopaths


class SkipAccessProtocolError(RuntimeError):
    """Error signalling access protocol should be skipped."""

    pass


class AccessProtocol(WritableMixin, namedtuple('AccessProtocol', ('DiracName',
                                                                  'VOPath',
                                                                  'Protocol',
                                                                  'PluginName',
                                                                  'Port',
                                                                  'Access',
                                                                  'Path',
                                                                  'SpaceToken',
                                                                  'WSUrl',
                                                                  'Host'))):
    """A Dirac Access Protocol."""

    __slots__ = ()

    def __new__(cls, vo, vo_info, se, port=None, protocol=None,
                plugin_name=None, ws_url=None, existing_access_protocols=None):
        """Constructor."""
        path = vo_info.get('Path')
        if path is None:
            gLogger.warn("No access protocol path determined for se: %s" % se)
            raise SkipAccessProtocolError()

        if existing_access_protocols is None:
            existing_access_protocols = ()

        vo_path = vo_info.get('VOPath', vo)
###########
        # off-spec behaviour
        if not vo_path.startswith(path):
            vo_path = os.path.join(path, vo_path)
###########
# This we shall comment out in order to replicate the off-spec behaviour of the original. Can at some
# point choose to put it in instead of above
#        if vo_path != vo and vo_path.startswith(path):
#            vo_path = vo_path[len(path):]

        return super(AccessProtocol, cls).__new__(cls,
                                                  DiracName='AccessProtocol.%s' % (len(existing_access_protocols) + 1),
                                                  VOPath={vo: vo_path},
                                                  Protocol=protocol,
                                                  PluginName=plugin_name,
                                                  Port=port,
                                                  Access='remote',
                                                  Path=path,
                                                  SpaceToken='',
                                                  WSUrl=ws_url,
                                                  Host=se)

# This we shall comment out in order to replicate the off-spec behaviour of the original. Can at some
# point choose to put it in.
#    def write(self, cfg_system, path_root):
#        """Write out config."""
#        old_path_var = gConfig.getValue(cfgPath(path_root, self.DiracName, 'Path'), self.Path)
#
#        if self.Path != old_path_var:
#            current_vo_paths = {vo: os.path.join(old_path_var, vo_path) for vo, vo_path in
#                                gConfig.getOptionsDict(cfgPath(path_root,
#                                                               self.DiracName,
#                                                               'VOPath')).get('Value', {}).iteritems()}
#
#            current_vo_paths.update({vo: os.path.join(self.Path, vo_path) for vo, vo_path in self.VOPath})
#            path, vo_paths = splitcommonvopaths(current_vo_paths)
#            return super(AccessProtocol, self._replace(Path=path,
#                                                       VOPath=vo_paths)).write(cfg_system, path_root)
#
#        return super(AccessProtocol, self).write(cfg_system, path_root)


class XRootDAccessProtocol(AccessProtocol):
    """XRootD Access Protocol."""

    __slots__ = ()

    def __new__(cls, vo, vo_info, se, xrootd_ports, existing_access_protocols=None):
        """Constructor."""
        if not xrootd_ports:
            gLogger.warn("No port determined for %s" % se)
            raise SkipAccessProtocolError()

        return super(XRootDAccessProtocol, cls).__new__(cls, vo, vo_info, se,
                                                        port=1094 if 1094 in xrootd_ports else min(xrootd_ports),
                                                        protocol='root',
                                                        plugin_name='GFAL2_XROOT',
                                                        existing_access_protocols=existing_access_protocols)


class SRMAccessProtocol(AccessProtocol):
    """SRM Access Protocol."""

    __slots__ = ()

    def __new__(cls, vo, vo_info, se, srm_dict, existing_access_protocols=None):
        """Constructor."""
        if not srm_dict:
            gLogger.warn("No SRM info for SE %s." % se)
            raise SkipAccessProtocolError()

        version = srm_dict.get('GlueServiceVersion', '')
        if not version.startswith('2'):
            gLogger.warn("Not SRM version 2 (%s)" % se)
            raise SkipAccessProtocolError()

        port = urlparse(srm_dict.get('GlueServiceEndpoint', '')).port
        if port is None:
            gLogger.warn("No port determined for %s" % se)
            raise SkipAccessProtocolError()
        return super(SRMAccessProtocol, cls).__new__(cls, vo, vo_info, se,
                                                     port=port,
                                                     protocol='srm',
                                                     plugin_name='GFAL2_SRM2',
                                                     ws_url='/srm/managerv2?SFN=',
                                                     existing_access_protocols=existing_access_protocols)


class SE(WritableMixin, namedtuple('SE', ('DiracName',
                                          'AccessProtocols',
                                          'Host',
                                          'BackendType',
                                          'Description',
                                          'VO',
                                          'TotalSize',
                                          'LastSeen'))):
    """A Dirac SE."""

    __slots__ = ()
    latency_mapping = {'online': 'disk',
                       'nearline': 'tape'}

    def __new__(cls, se, se_info, srms, xrootd_ports, vo, vo_info, existing_ses=None):
        """Constructor."""
        if existing_ses is None:
            existing_ses = {}
        bdii_site_id = se_info.get('GlueSiteUniqueID')
        se_latency = SE.latency_mapping.get(se_info.get('GlueSAAccessLatency', 'online').lower(),
                                            'disk')
        dirac_name = None
        matching_ses = {se: host for se, host in existing_ses.iteritems()\
                        if se.startswith(bdii_site_id) and se.endswith(se_latency)}
        for dirac_sename, hostname in matching_ses:
            if hostname == se:
                dirac_name = dirac_sename

        if dirac_name is None:
            count = len(matching_ses)
            dirac_name = '%s%s-%s' % (bdii_site_id,
                                      count or '',
                                      se_latency)

        srm_dict = srms.get(se)
        # DIRACs Bdii2CSAgent used the ServiceAccessControlBaseRule value
        base_rules = srm_dict.get('GlueServiceAccessControlBaseRule', []) if srm_dict\
            else se_info.get('GlueSAAccessControlBaseRule', [])
        if not isinstance(base_rules, (list, tuple)):
            base_rules = [base_rules]

        bdii_vos = set(re.sub('^VO:', '', rule) for rule in base_rules)

        aps = []
        try:
            aps.append(SRMAccessProtocol(vo, vo_info, se, srm_dict, aps))
        except SkipAccessProtocolError:
            gLogger.info("Skipping SRM access protocol for se: %s" % se)

        try:
            aps.append(XRootDAccessProtocol(vo, vo_info, se, xrootd_ports, aps))
        except SkipAccessProtocolError:
            gLogger.info("Skipping XRootD access protocol for se: %s" % se)

        return super(SE, cls).__new__(cls,
                                      DiracName=dirac_name,
                                      AccessProtocols=tuple(aps),
                                      Host=se if not aps else None,
                                      BackendType=se_info.get('GlueSEImplementationName', 'Unknown'),
                                      Description=se_info.get('GlueSEName'),
                                      VO=', '.join(sorted(bdii_vos)),
                                      TotalSize=se_info.get('GlueSETotalOnlineSize', 'Unknown'),
                                      LastSeen=date.today().strftime('%d/%m/%Y'))

__all__ = ('SE', 'SRMAccessProtocol', 'XRootDAccessProtocol')
