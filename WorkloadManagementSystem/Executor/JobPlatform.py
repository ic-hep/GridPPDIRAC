""" JobPlatform Executor for GridPP DIRAC.

    The JobPlatform Executor sets the default platform of a user job if none
    was specified at submission time.

    If the user specifies "Any" as the platform then the original behaviour
    of matching any platform is used (by removing the platform option entirely
    from the JDL).
"""

__RCSID__ = "$Id$"


from DIRAC import S_ERROR
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor  import OptimizerExecutor


class JobPlatform( OptimizerExecutor ):
  """ An executor for setting a job's default platform. """

  def optimizeJob( self, jid, jobState ):
    """ Process the job and set the platform if needed. """

    def_plat = self.ex_getOption( 'GridPP_DefaultPlatform', '' )
    if not def_plat:
      # No default platform set, so don't do anything
      return self.setNextOptimizer( jobState )

    result = jobState.getManifest()
    if not result["OK"]:
      # Failed to get the job manifest?
      self.jobLog.error( "Failed to get job manifest." )
      return S_ERROR( "Failed to get job manifest." ) 

    manifest = result["Value"]
    job_plat = manifest.getOption( "Platform" )
    result = manifest.getSection( "JobRequirements" )
    if not result[ 'OK' ]:
      self.jobLog.error( "Failed to get job requirements." )
      return S_ERROR( "Failed to get job requirements." ) 
    requirements = result[ 'Value' ]

    if job_plat and job_plat.lower() == "anyplatform":
      # User really wants _any_ platform, so remove the option
      manifest.remove( "Platform" )
      try:
        requirements.deleteKey( "Platforms" )
      except KeyError:
        pass # If Platforms doesn't exist, it doesn't matter.
      self.jobLog.info( "Removed job platform." )
    elif not job_plat:
      # User didn't set platform, user default
      job_plat = def_plat
      manifest.setOption( "Platform", def_plat )
      # We also have to set the platform in the job requirements
      requirements.setOption( "Platforms", def_plat )
      self.jobLog.info( "Set job platform to default (%s)." % def_plat )

    return self.setNextOptimizer( jobState )

