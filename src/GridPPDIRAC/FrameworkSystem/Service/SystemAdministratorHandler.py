"""GridPP SystemAdministratorHandler - Remove some dangerous functions."""

from DIRAC import S_ERROR, S_OK
from DIRAC.FrameworkSystem.Service.SystemAdministratorHandler import SystemAdministratorHandler as OrigSysAdminHandler


class SystemAdministratorHandler(OrigSysAdminHandler):

    def export_executeCommand(self, *args, **kwargs):
        return S_ERROR("Function disabled")

    def export_updateSoftware(self, *args, **kwargs):
        return S_ERROR("Function disabled")

    def export_revertSoftware(self, *args, **kwargs):
        return S_ERROR("Function disabled")

