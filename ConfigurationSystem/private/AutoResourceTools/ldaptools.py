"""Mock the Python ldap module API"""
import os
import re
import shlex
import subprocess
import warnings


__all__ = ("MockLdap", "in_")

with open(os.devnull, 'wb') as devnull:
    try:
        subprocess.call(['ldapsearch', "-h"], stdout=devnull, stderr=subprocess.STDOUT)
    except OSError:
        warnings.warn("Problem calling to ldapsearch, binary may be missing.", RuntimeWarning)


class MockLdap(object):
    """Mock of the ldap connection object."""

    entry_regex = re.compile(r"^dn: ([^\n]*)$\n(.*?)$(?=^\s*$)", re.MULTILINE | re.DOTALL)
    option_regex = re.compile(r"(^[^:]+): (.*)$", re.MULTILINE)
    SCOPE_SUBTREE = None

    def __init__(self, hostname, port):
        self._host = ':'.join((hostname, str(port)))

    @classmethod
    def open(cls, hostname, port):
        """Open connection mock."""
        return cls(hostname, port)

    def search_s(self, base, filterstr, scope=None):
        """
        Mimic the return from the ldap search_s API as not available in DiracOS.

        Args:
            base (str): base
            filterstr (str): filters
            scope (*): unused at this point

        Returns:
            list: list of (dn, attib_dict) for items matching the filterstr
        """
        cmd = "ldapsearch -x -LLL -o ldif-wrap=no -h {host} -b {base!r} {filterstr!r}"
        stdout = subprocess.check_output(shlex.split(cmd.format(host=self._host,
                                                                base=base,
                                                                filterstr=filterstr)))
        return [(dn, dict(MockLdap.option_regex.findall(options)))
                for dn, options in MockLdap.entry_regex.findall(stdout)]


def in_(attrs, iterable):
    """
    Helper function for generating ldap filter strings from an iterable.

    This function generated an ldap filter string that requires that the attribute given in the
    attrs parameter is in given iterable. If attrs is a list/tuple of attributes then it is required
    that they both match those given in iterable.

    Parameters:
        attrs (str/list): The parameter(s) that we will be filtering.
        iterable (list): This is a list of single strings or a tuples of size len(attrs) if attrs is
                         a list.
    """
    if isinstance(attrs, basestring):
        return "(|(" + ')('.join('='.join((attrs, value)) for value in iterable) + "))"

    inner_join = lambda values: ''.join(("(&(",
                                         ')('.join('='.join(filt) for filt in zip(attrs, values)),
                                         "))"))
    return "(|" + ''.join(inner_join(values) for values in iterable) + ")"
