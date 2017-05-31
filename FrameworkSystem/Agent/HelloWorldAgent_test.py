""" Travis test for HelloWordAgent

"""

from DIRAC import S_OK, S_ERROR
from GridPPDIRAC.FrameworkSystem.Agent.HelloWorldAgent import HelloWorldAgent

__RCSID__ = "Id: $"

def test_basic():
    print 'Hello world'
    assert 1

    
