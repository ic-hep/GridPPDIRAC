"""Dirac multiVO utilities."""
import os
from collections import defaultdict
from urlparse import urlparse
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.Grid import ldapSEAccessProtocol, ldapsearchBDII


class WritableMixin(object):
    """
    Mixin class for writing out Dirac config named tuples.

    Note: This class is expected to be used as a mix-in class with one already extending
          namedtuple and also expects there to be a property named 'DiracName'.
    """

    __slots__ = ()

    def write(self, cfg_system, path_root):
        """
        Write out config.

        Args:
            cfg_system (ConfigurationSystem): The ConfigurationSystem object that will handle
                                              the writing to the DIRAC CS.
            path_root (str): The path in the DIRAC CS to write options to.
        """
        path = cfgPath(path_root, self.DiracName)

        for option, value in self._replace(DiracName=None)._asdict().iteritems():
            if isinstance(value, list):
                for val in value:
                    val.write(cfg_system, cfgPath(path, option))
            elif isinstance(value, tuple):
                for val in value:
                    val.write(cfg_system, path)
            elif isinstance(value, WritableMixin):
                value.write(cfg_system, path)
            elif isinstance(value, dict):
                for key, val in value.iteritems():
                    cfg_system.add(cfgPath(path, option), key, val)
            elif value is not None:
                cfg_system.add(path, option, value)


def splitcommonvopaths(vo_paths):
    """
    Split VO paths.

    Return the common root between different VO paths along with the dict of VO name
    to VO path trimmed to be relative to the root.

    Note: There is a builtin function os.path.commonpath in Python > 3.5 which
          can get the root part.

    Args:
        vo_paths (dict): Dictionary of vo name to vo path from which to find the root

    Returns:
        tuple(str, dict): Tuple containing the common root as the first element
                          and the dict of vo name to vo path without the root as the
                          second
    """
    root = os.path.dirname(os.path.commonprefix([i if i.endswith(os.sep) else i + os.sep
                                                 for i in vo_paths.itervalues()]))
    return root, {vo: path[len(root):].strip(os.sep) for vo, path in vo_paths}


def get_xrootd_ports(se, host):
    """
    Get DBII XRootD ports.

    Args:
        se (str): The SE to get XRootD ports for.
        host (str): The BDII host.

    Returns:
        set: The XRootD ports defined in the BDII
    """
    result = ldapSEAccessProtocol(se, host=host)
    return set(port for protocol_type, port
               in ((i.get('GlueSEAccessProtocolType', '').lower(),
                    urlparse(i.get('GlueSEAccessProtocolEndpoint', '')).port) for i in result.get('Value', ()))
               if 'root' in protocol_type and port is not None)


def get_se_vo_info(vo_name, host=None):
    """
    function for getting dict of SE: VO info path.

    Args:
        vo_name (str): The VO that we want the mapping for.
        host (str): The BDII host.

    Returns:
        dict: A mapping of SE name to VO Paths.
    """
    vo_filter = '(GlueVOInfoAccessControlBaseRule=VOMS:/%s/*)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlBaseRule=VOMS:/%s)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlBaseRule=VO:%s)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlBaseRule=%s)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlRule=VOMS:/%s/*)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlRule=VOMS:/%s)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlRule=VO:%s)' % vo_name
    vo_filter += '(GlueVOInfoAccessControlRule=%s)' % vo_name
    filt = '(&(objectClass=GlueVOInfo)(|%s))' % vo_filter
    result = ldapsearchBDII(filt=filt, host=host)

    paths_mapping = defaultdict(set)
    for se_info in result.get('Value', ()):
        for elem in se_info.get('attr', {}).get('GlueChunkKey', []):
            if 'GlueSEUniqueID=' in elem:
                paths_mapping[elem.replace('GlueSEUniqueID=', '')].add(se_info['attr']['GlueVOInfoPath'])

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
        dirname = os.path.dirname(norm_path)
        ret[se_name] = {'Path': dirname}
        if os.path.join(dirname, vo_name) != norm_path:
            ret[se_name].update({'VOPath': norm_path})
    return ret

__all__ = ('WritableMixin', 'splitcommonvopaths', 'get_xrootd_ports', 'get_se_vo_info')
