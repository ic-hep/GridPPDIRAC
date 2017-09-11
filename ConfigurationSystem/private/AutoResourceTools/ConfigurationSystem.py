"""Dirac multiVO Configuration system."""
from types import GeneratorType
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI


class ConfigurationSystem(CSAPI):
    """ Class to smartly wrap the functionality of the CS"""

    def __init__(self):
        """initialise"""
        CSAPI.__init__(self)
        self._num_changes = 0
        result = self.initialize()
        if not result['OK']:
            gLogger.error('Failed to initialise CSAPI object:',
                          result['Message'])
            raise RuntimeError(result['Message'])

    def add(self, section, option, new_value):
        """
        Add a value into the configuration system.

        This method will overwrite any existing option's value.

        Args:
            section (str): The section
            option (str): The option to be created/modified
            new_value: The value to be assigned

        Example:
            >>> cs = ConfigurationSystem()
            >>> cs.add('/Registry', 'DefaultGroup', 'dteam_user')
        """
        if isinstance(new_value, (tuple, list, set, GeneratorType)):
            new_value = ', '.join(sorted(map(str, new_value)))
        else:
            new_value = str(new_value)

        old_value = gConfig.getValue(cfgPath(section, option), None)
        if old_value == new_value:
            return

        if old_value is None:
            gLogger.notice("Setting %s/%s:   -> %s"
                           % (section, option, new_value))
            self.setOption(cfgPath(section, option), new_value)
        else:
            gLogger.notice("Modifying %s/%s:   %s -> %s"
                           % (section, option, old_value, new_value))
            self.modifyValue(cfgPath(section, option), new_value)
        self._num_changes += 1

    def append_unique(self, section, option, new_value):
        """
        Append a value onto the end of an existing CS option.

        This method is like append except that it ensures that the final list
        of values for the given option only contains unique entries.
        """
#        old_values = set(v.strip() for v in gConfig.getValue(cfgPath(section, option), '').split(',') if v)
        old_values = (v.strip() for v in gConfig.getValue(cfgPath(section, option), '').split(','))
        new_values = set(v for v in old_values if v)

        if isinstance(new_value, (tuple, list, set, GeneratorType)):
            new_values.update(map(str, new_value))
        else:
            new_values.add(str(new_value))
        self.add(section, option, new_values)

    def append(self, section, option, new_value):
        """
        Append a value onto the end of an existing CS option.

        This method is like add with the exception that the new value
        is appended on to the end of the list of values associated
        with that option.
        """
#        old_values = [v.strip() for v in gConfig.getValue(cfgPath(section, option), '').split(',') if v]
        old_values = (v.strip() for v in gConfig.getValue(cfgPath(section, option), '').split(','))
        new_values = [v for v in old_values if v]

        if isinstance(new_value, (tuple, list, set, GeneratorType)):
            new_values.extend(new_value)
        else:
            new_values.append(new_value)
        self.add(section, option, new_values)

    def remove(self, section, option=None, value=None):
        """
        Remove a section/option from the configuration system.

        This method will remove the specified section if the option argument
        is None (default). If the option argument is given but value is None
        then that option (formed of section/option) is removed. If both option
        and value are given then that value is removed from the comma seperated
        values associated with that option.

        Args:
            section (str): The section
            option (str): [optional] The option to remove
            value (str): [optional] The value to remove

        Example:
            >>> ConfigurationSystem().remove('/Registry', 'DefaultGroup')
        """
        if option is None:
            gLogger.notice("Removing section %s" % section)
            self.delSection(section)
            self._num_changes += 1
        elif value is None:
            gLogger.notice("Removing option %s/%s" % (section, option))
            self.delOption(cfgPath(section, option))
            self._num_changes += 1
        else:
            if isinstance(value, str):
                value = [value]
            gLogger.notice("Removing value(s) %s from option %s/%s"
                           % (list(value), section, option))
            old_values = (v.strip() for v in gConfig.getValue(cfgPath(section, option), '').split(','))
            new_values = [v for v in old_values if v and v not in value]
            self.add(section, option, new_values)

    def commit(self):
        """
        Commit the changes to the configuration system.

        Returns:
            dict: S_OK/S_ERROR DIRAC style dicts
        """
        result = CSAPI.commit(self)
        if not result['OK']:
            gLogger.error("Error while commit to CS", result['Message'])
            return S_ERROR("Error while commit to CS")
        if self._num_changes:
            gLogger.notice("Successfully committed %d changes to CS\n"
                           % self._num_changes)
            self._num_changes = 0
            return S_OK()
        gLogger.notice("No changes to commit")
        return S_OK()

__all__ = ('ConfigurationSystem',)
