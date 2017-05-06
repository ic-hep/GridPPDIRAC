""" The GridPP Matcher service.

    This provides a small wrapper around the DIRAC standard matcher,
    it allows re-writing of a node's dirac-platform to a more friendly name
    i.e. EL6 rather than Linux_x86_64_glibc-2.12.
    The platforms are set via the GridPP_NodeCompatibility section in 
    Resources/Computing. A non-matched platform is kept unchanged.
"""

__RCSID__ = "$Id$"


from DIRAC import gConfig, gLogger

from DIRAC.WorkloadManagementSystem.Service.MatcherHandler import initializeMatcherHandler as initializeCoreMatcherHandler
from DIRAC.WorkloadManagementSystem.Service.MatcherHandler import MatcherHandler as CoreMatcherHandler


def initializeMatcherHandler( serviceInfo ):
  return initializeCoreMatcherHandler( serviceInfo )


class MatcherHandler( CoreMatcherHandler ):
  """ This class is just a thin wrapper around the main DIRAC Matcher service. """

  def __patchPlatform( self, resourceDescription ):
    """ Replace the Platform field in a resource dictionary if it exists and
        a mapping exists in the DIRAC config system. """

    # Load a dict of the platforms to map
    result = gConfig.getOptionsDict( '/Resources/Computing/GridPP_NodeCompatibility' )
    if not result['OK']:
      # Fail to load dictionary
      gLogger.error( "Failed to load NodeCompatibility" )
      return resourceDescription
    plats = result['Value']

    # Try to map the platform the node reported if it's in the dict
    if 'Platform' in resourceDescription:
      node_plat = resourceDescription['Platform']
      for plain_plat, dirac_plat in plats.items():
        if dirac_plat == node_plat:
          node_plat = plain_plat
          break
      resourceDescription['Platform'] = node_plat

    return resourceDescription


  def export_requestJob( self, resourceDescription ):
    resourceDescription = self.__patchPlatform( resourceDescription )
    return CoreMatcherHandler.export_requestJob( self, resourceDescription )


  def export_getMatchingTaskQueues( self, resourceDict ):
    resourceDict = self.__patchPlatform( resourceDict )
    return CoreMatcherHandler.export_getMatchingTaskQueues( self, resourceDict )


  def export_matchAndGetTaskQueue( self, resourceDict ):
    resourceDict = self.__patchPlatform( resourceDict )
    return CoreMatcherHandler.export_matchAndGetTaskQueue( self, resourceDict )


