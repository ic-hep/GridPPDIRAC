""" :mod: Hello World Agent

    Prints Hello to the log every cycle.
"""

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule

__RCSID__ = "Id: $"

class HelloWorldAgent( AgentModule ):
  """
  .. class:: HelloWorldAgent

  Prints hello world on the log.
  """

  def initialize( self ):
    """ Init the agent

    :param self: self reference
    """
    self.hello_name = self.am_getOption( "HelloName", "World" )
    return S_OK()

  def execute( self ):
    """ Prints hello to the log.

    :param self: self reference
    """
    self.log.info( "Hello %s!" % self.hello_name )
    return S_OK()

