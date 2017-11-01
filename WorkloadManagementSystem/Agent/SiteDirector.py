########################################################################
# $HeadURL$
# File :    SiteDirector.py
# Author :  A.T.
########################################################################

"""  The Site Director is a simple agent performing pilot job submission to particular sites.
"""

import os
import DIRAC
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import pilotAgentsDB
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector as OriginalSiteDirector

DIRAC_MODULES = [ os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotCommands.py' ),
                  os.path.join( DIRAC.rootPath, 'GridPPDIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'GridPPCommands.py' ),
                  os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotTools.py' ) ]

class SiteDirector( OriginalSiteDirector ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """


  def beginExecution( self ):
    # Get the extra pilot options for this SiteDirector instance
    result = OriginalSiteDirector.beginExecution(self)
    self.extraModules = self.am_getOption( 'ExtraPilotModules', [] ) + DIRAC_MODULES
    self.extraOptions = self.am_getOption( "ExtraPilotOptions", '' )
    return result

  def __getQueueSlots( self, queue ):
    """ Get the number of available slots in the queue
    """
    ce = self.queueDict[queue]['CE']
    ceName = self.queueDict[queue]['CEName']
    queueName = self.queueDict[queue]['QueueName']

    self.queueSlots.setdefault( queue, {} )
    totalSlots = self.queueSlots[queue].get( 'AvailableSlots', 0 )
    availableSlotsCount = self.queueSlots[queue].setdefault( 'AvailableSlotsCount', 0 )
    if totalSlots == 0:
      if availableSlotsCount % 10 == 0:
        
        # Get the list of already existing pilots for this queue
        jobIDList = None
        result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                              'Queue':queueName,
                                              'Status':['Running','Submitted','Scheduled'] } )
        if result['OK'] and result['Value']:
          jobIDList = result['Value']
          
        result = ce.available( jobIDList )
        if not result['OK']:
          self.log.warn( 'Failed to check the availability of queue %s: \n%s' % ( queue, result['Message'] ) )
          self.failedQueues[queue] += 1
        else:
          ceInfoDict = result['CEInfoDict']
          self.log.info( "CE queue report(%s_%s): Wait=%d, Run=%d, Submitted=%d, Max=%d" % \
                         ( ceName, queueName, ceInfoDict['WaitingJobs'], ceInfoDict['RunningJobs'],
                           ceInfoDict['SubmittedJobs'], ceInfoDict['MaxTotalJobs'] ) )
          totalSlots = result['Value']
          self.queueSlots[queue]['AvailableSlots'] = totalSlots

    self.queueSlots[queue]['AvailableSlotsCount'] += 1
    return totalSlots

  def _getPilotOptions( self, queue, pilotsToSubmit, processors=1):
    """ Prepare pilot options
    """
    pilotOptions, pilotsToSubmit = OriginalSiteDirector._getPilotOptions(self, queue, pilotsToSubmit, processors)
    # Get the module specific options
    if self.extraOptions:
      pilotOptions.append( self.extraOptions )
    return [pilotOptions, pilotsToSubmit]
