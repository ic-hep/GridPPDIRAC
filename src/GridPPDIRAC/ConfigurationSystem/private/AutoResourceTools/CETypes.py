"""Dirac multi VO site types."""
import re
from datetime import date
from collections import namedtuple
from DIRAC import gConfig
from .utils import WritableMixin


class NotIncludedError(Exception):
    pass

# TODO: This does not handle Glue2 properly
class Site(WritableMixin, namedtuple('Site', ('DiracName',
                                              'Name',
                                              'CEs',
                                              'Description',
                                              'Coordinates',
                                              'Mail',
                                              'CE',
                                              'SE'))):
    """A Dirac Site."""

    __slots__ = ()
    cc_regex = re.compile(r'\.([a-zA-Z]{2})$')
    cc_mappings = {'.gov': 'us',
                   '.edu': 'us',
                   'efda.org': 'uk',
                   'atlas-swt2.org': 'us'}

    def __new__(cls, site, site_info_lst, domain='LCG', country_default='xx', banned_ces=None, max_processors=None):
        """Constructor."""
        ces = []
        ce_list = set()
        country_code = country_default
        # We have to collect CE names across all VOs
        for site_info in site_info_lst:
            for ce, ce_info in sorted(site_info.get('CEs', {}).items()):
               if banned_ces is not None and ce in banned_ces:
                   continue

               if country_code == country_default:
                   country_code = Site.extract_cc(ce) or country_default
        # We think this causes the extra half configured 'condor' queues to appear
        # TODO (apart from the complete rewrite: make sure we can still ban CEs from the autoconfig
        #       try:
        #           ces.append(CE(ce, ce_info, max_processors))
        #           ce_list.add(ce)
        #       except NotIncludedError:
        #           pass

        se_list = set(se for se in gConfig.getSections('/Resources/StorageElements').get('Value', [])
                      if se.startswith(site))
        # Work around glue1 to glue2 transition
        site_name = site
        # TODO: Coordinates and Mail need to be taken from the GOCDB
        # Description can be dropped completely, but care needs to be taken that nothing else expects it
        return super(Site, cls).__new__(cls,
                                        DiracName='.'.join((domain, site, country_code)),
                                        Name=site_name,
                                        CEs=ces,
                                        Description='LCG site',
                                        Coordinates='0.0:0.0',
                                        Mail='contact_info@GOCDB',
                                        CE=ce_list,
                                        SE=se_list)



    @classmethod
    def extract_cc(cls, ce, cc_mappings=None, cc_regex=None):
        """Extract the 2 character country code from the CE name."""
        if cc_mappings is None:
            cc_mappings = cls.cc_mappings
        if cc_regex is None:
            cc_regex = cls.cc_regex

        ce = ce.strip().lower()
        for key, value in cc_mappings.items():
            if ce.endswith(key):
                return value
        cc = cc_regex.search(ce)
        if cc is not None:
            cc = cc.groups()[0]
        return cc


class CE(WritableMixin, namedtuple('CE', ('DiracName',
                                          'Queues',
                                          'MaxProcessors',
                                          'LastSeen',
                                          'architecture',
                                          'SI00',
                                          'HostRAM',
                                          'CEType',
                                          'OS',
                                          'SubmissionMode',
                                          'JobListFile'))):
    """A Dirac CE."""

    __slots__ = ()

    def __new__(cls, ce, ce_info, max_processors=None):
        """Constructor."""
        queues = []
        ce_type = ''
        ce_logical_cpus = int(ce_info.get('GlueSubClusterLogicalCPUs', 0))
        ce_si00 = ce_info.get('GlueHostBenchmarkSI00', '')
        for queue, queue_info in sorted(ce_info.get('Queues', {}).items()):
            queues.append(Queue(queue, queue_info, ce_logical_cpus, ce_si00))
            ce_type = queue_info.get('GlueCEImplementationName', '')

        if ce_type == 'ARC-CE':
            raise NotIncludedError("Excluding ARC CEs")

        num_cores = int(max_processors or ce_info.get('GlueHostArchitectureSMPSize', 1))

        return super(CE, cls).__new__(cls,
                                      DiracName=ce,
                                      Queues=queues,
                                      MaxProcessors=num_cores if num_cores > 1 else None,
                                      LastSeen=date.today().strftime('%d/%m/%Y'),
                                      architecture=ce_info.get('GlueHostArchitecturePlatformType', ''),
                                      SI00=ce_si00,
                                      HostRAM=ce_info.get('GlueHostMainMemoryRAMSize', ''),
                                      CEType='ARC' if ce_type == 'ARC-CE' else ce_type,
                                      OS='EL' + ce_info.get('GlueHostOperatingSystemRelease', '').split('.')[0].strip(),
                                      SubmissionMode='Direct' if 'ARC' in ce_type or 'CREAM' in ce_type else None,
                                      JobListFile='%s-jobs.xml' % ce if 'ARC' in ce_type else None)


class Queue(WritableMixin, namedtuple('Queue', ('DiracName',
                                                'VO',
                                                'SI00',
                                                'maxCPUTime',
                                                'MaxTotalJobs',
                                                'MaxWaitingJobs'))):
    """A Dirac Queue."""

    __slots__ = ()

    def __new__(cls, queue, queue_info, ce_logical_cpus=0, ce_si00=0):
        """Constructor."""
        max_cpu_time = queue_info.get('GlueCEPolicyMaxCPUTime')
        if max_cpu_time == "0" or max_cpu_time == "2147483647":
            # bug on arc or Batch system integration is broken at site, hard code to 2 days
            max_cpu_time = "2880"
        elif max_cpu_time is None:
            max_cpu_time = '0'

        vo = set()
        if queue_info.get('GlueCEStateStatus', '').lower() == 'production':
            acbr = queue_info.get('GlueCEAccessControlBaseRule')
            if not isinstance(acbr, (list, tuple, set)):
                acbr = [acbr]
            vo.update(rule.replace('VO:', '') for rule in acbr if rule and rule.startswith('VO:'))

        si00 = ''
        capability = queue_info.get('GlueCECapability', [])
        if isinstance(capability, str):
            capability = [capability]
        for i in capability:
            if 'CPUScalingReferenceSI00' in i:
                si00 = i.split('=')[-1].strip()
                break
        if not si00:
            # We couldn't get an SI00 value for this queue
            # Inherit the value from the host
            si00 = ce_si00

        # MaxTotalJobs in dirac is (running jobs (i.e. hardware) + waiting jobs)
        max_total_jobs_slots = int(queue_info.get('GlueCEInfoTotalCPUs', 0)) or ce_logical_cpus
        # changed MaxWaitingJobs from 2*max_total_jobs_slots after complaints about too many pilot jobs
        return super(Queue, cls).__new__(cls,
                                         DiracName=queue,
                                         VO=', '.join(sorted(vo)),
                                         SI00=si00,
                                         maxCPUTime=max_cpu_time,
                                         MaxTotalJobs=3*max_total_jobs_slots,
                                         MaxWaitingJobs=max_total_jobs_slots)

__all__ = ('Site', 'CE', 'Queue')
