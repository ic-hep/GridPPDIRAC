""" We have to patch the MPSiteDirector as just patching the base class
    doesn't work.
"""

import os
import DIRAC
from DIRAC.WorkloadManagementSystem.Agent.MultiProcessorSiteDirector import MultiProcessorSiteDirector as OriginalMPSiteDirector

DIRAC_MODULES = [ os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotCommands.py' ),
                  os.path.join( DIRAC.rootPath, 'GridPPDIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'GridPPCommands.py' ),
                  os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotTools.py' ) ]

class MultiProcessorSiteDirector( OriginalMPSiteDirector ):

  def beginExecution( self ):
    # Get the extra pilot options for this SiteDirector instance
    result = OriginalMPSiteDirector.beginExecution(self)
    self.extraModules = self.am_getOption( 'ExtraPilotModules', [] ) + DIRAC_MODULES
    self.extraOptions = self.am_getOption( "ExtraPilotOptions", '' )
    return result

  def _getPilotOptions( self, queue, pilotsToSubmit, **kwargs ):
    """ Prepare pilot options
    """
    pilotOptions, pilotsToSubmit = OriginalMPSiteDirector._getPilotOptions(self, queue, pilotsToSubmit)
    # Get the module specific options
    if self.extraOptions:
      pilotOptions.append( self.extraOptions )
    processors = kwargs.pop('processors', 1)
    if processors > 0:
      pilotOptions.append( '--maxNumberOfProcessors %u' % processors )
      pilotOptions.append( '--requiredTag %uProcessors' % processors )
    return [pilotOptions, pilotsToSubmit]

