""" Travis test for HelloWordAgent

"""

from DIRAC import S_OK, S_ERROR
from GridPPDIRAC.FrameworkSystem.Agent.HelloWorld import HelloWorldAgent

__RCSID__ = "Id: $"

def test_basic():
    print 'Hello world'
    assert 1

    
