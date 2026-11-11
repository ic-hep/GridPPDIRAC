"""GreenDigit agent template."""

from datetime import datetime, timedelta, timezone
import json
import os

import requests

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client import JobStatus


__RCSID__ = "$Id$"

# Collector field name -> JobParametersDB key.
PARAMETER_FIELD_MAP = {
	"ModelName": "ModelName",
	"CPUNormalizationFactor": "CPUNormalizationFactor",
	"HostName": "HostName",
	"JobType": "JobType",
	"LoadAverage": "LoadAverage",
	"MemoryUsed(kb)": "MemoryUsed(kb)",
	"NormCPUTime(s)": "NormCPUTime(s)",
	"ScaledCPUTime(s)": "ScaledCPUTime(s)",
	"TotalCPUTime(s)": "TotalCPUTime(s)",
	"WallClockTime(s)": "WallClockTime(s)",
	"DiskSpace(MB)": "DiskSpace(MB)",
	"CEQueue": "CEQueue",
	"GridCE": "GridCE",
}

# Collector field name -> JobDB Jobs attribute.
ATTRIBUTE_FIELD_MAP = {
	"JobGroup": "JobGroup",
	"JobName": "JobName",
	"Owner": "Owner",
	"OwnerDN": "OwnerDN",
	"OwnerGroup": "OwnerGroup",
	"RescheduleCounter": "RescheduleCounter",
	"Site": "Site",
	"Status": "Status",
	"SubmissionTime": "SubmissionTime",
	"StartExecTime": "StartExecTime",
	"EndExecTime": "EndExecTime",
}


class GreenDigitAgent(AgentModule):
	"""Standard DIRAC agent scaffold for GreenDigit tasks."""

	def initialize(self):
		"""Initialize agent state and configuration."""
		self.am_setOption("PollingTime", 15 * 60)
		self.collector_base_url = self.am_getOption(
			"CollectorBaseURL", "https://dirac.egi.eu/job-reporting"
		)
		self.collector_api_key = self.am_getOption("CollectorAPIKey", "")
		self.dry_run = self.am_getOption("DryRun", False)
		self.collector_ca_path = self.am_getOption(
			"CollectorCAPath", os.environ.get("X509_CERT_DIR")
		)
		self.collector_fields_refresh_cycles = int(
			self.am_getOption("CollectorFieldsRefreshCycles", 2)
		)
		if self.collector_fields_refresh_cycles < 1:
			return S_ERROR("CollectorFieldsRefreshCycles must be at least 1")
		self.collector_job_fields = None
		self.cycles_since_fields_refresh = 0
		self.timestamp_file = os.path.join(self.am_getWorkDirectory(), "green_digit_last_end_time")
		self.job_monitoring_client = JobMonitoringClient()
		return S_OK()

	def get_collector_health(self):
		"""Return the collector health response."""
		if not self.collector_ca_path:
			return S_ERROR(
				"IGTF CA bundle is not configured; set CollectorCAPath or X509_CERT_DIR"
			)

		url = "%s/health" % self.collector_base_url.rstrip("/")
		try:
			response = requests.get(
				url,
				timeout=10,
				verify=self.collector_ca_path,
				allow_redirects=False,
			)
		except requests.RequestException as error:
			self.log.exception("Failed to get GreenDigit collector health")
			return S_ERROR(str(error))

		if not 200 <= response.status_code < 300:
			message = "Collector health request failed with HTTP %s: %s" % (
				response.status_code, response.text
			)
			self.log.error(message)
			return S_ERROR(message)

		try:
			health = response.json()
		except ValueError:
			health = response.text
		return S_OK(health)

	def get_collector_job_fields(self):
		"""Return the collector's accepted job parameter and attribute names."""
		if not self.collector_ca_path:
			return S_ERROR(
				"IGTF CA bundle is not configured; set CollectorCAPath or X509_CERT_DIR"
			)

		url = "%s/job-fields" % self.collector_base_url.rstrip("/")
		try:
			response = requests.get(
				url,
				timeout=10,
				verify=self.collector_ca_path,
				allow_redirects=False,
			)
		except requests.RequestException as error:
			self.log.exception("Failed to get GreenDigit collector job fields")
			return S_ERROR(str(error))

		if not 200 <= response.status_code < 300:
			message = "Collector job-fields request failed with HTTP %s: %s" % (
				response.status_code, response.text
			)
			self.log.error(message)
			return S_ERROR(message)

		try:
			fields = response.json()
		except ValueError as error:
			message = "Collector job-fields response is not valid JSON: %s" % error
			self.log.error(message)
			return S_ERROR(message)

		required_lists = ("job_parameter_keys", "job_attribute_keys")
		if not isinstance(fields, dict) or any(
			not isinstance(fields.get(key), list)
			or any(not isinstance(name, str) for name in fields[key])
			for key in required_lists
		):
			message = "Collector job-fields response has an invalid structure"
			self.log.error(message)
			return S_ERROR(message)

		return S_OK(fields)

	@staticmethod
	def _json_value(value, timespec="auto"):
		"""Convert DIRAC DB values to JSON-compatible values."""
		if (
			isinstance(value, str)
			and len(value) >= 19
			and value[4] == "-"
			and value[7] == "-"
			and value[10] in ("T", " ")
		):
			try:
				value = datetime.fromisoformat(value.replace("Z", "+00:00"))
			except ValueError:
				pass
		if isinstance(value, datetime):
			if value.tzinfo is None:
				value = value.replace(tzinfo=timezone.utc)
			else:
				value = value.astimezone(timezone.utc)
			return value.isoformat(timespec=timespec).replace("+00:00", "Z")
		if isinstance(value, bytes):
			return value.decode("utf-8")
		return value

	@classmethod
	def build_payload(cls, job_id, attributes, parameters, collector_fields):
		"""Build a collector payload using explicit DIRAC-to-collector field maps."""
		accepted_parameters = set(collector_fields["job_parameter_keys"])
		accepted_attributes = set(collector_fields["job_attribute_keys"])
		if "JobID" not in accepted_parameters:
			return S_ERROR("Collector job-fields does not accept required job_parameters.JobID")

		job_parameters = {"JobID": int(job_id)}
		for collector_name, source_name in PARAMETER_FIELD_MAP.items():
			value = parameters.get(source_name)
			if collector_name in accepted_parameters and value is not None:
				job_parameters[collector_name] = cls._json_value(value)

		job_attributes = {}
		for collector_name, source_name in ATTRIBUTE_FIELD_MAP.items():
			value = attributes.get(source_name)
			if collector_name in accepted_attributes and value is not None:
				job_attributes[collector_name] = cls._json_value(value, timespec="seconds")

		return S_OK({"job_parameters": job_parameters, "job_attributes": job_attributes})

	@staticmethod
	def _parse_timestamp(value):
		"""Parse a checkpoint or monitoring-client timestamp as naive UTC."""
		if isinstance(value, str):
			value = datetime.fromisoformat(value.replace("Z", "+00:00"))
		if not isinstance(value, datetime):
			raise ValueError("EndExecTime is not a datetime")
		if value.tzinfo is not None:
			value = value.astimezone(timezone.utc).replace(tzinfo=None)
		return value

	def _read_checkpoint(self, now):
		try:
			with open(self.timestamp_file, encoding="ascii") as checkpoint:
				return self._parse_timestamp(checkpoint.read().strip())
		except FileNotFoundError:
			return now - timedelta(minutes=15)
		except (OSError, ValueError) as error:
			return S_ERROR("Could not read GreenDigit timestamp: %s" % error)

	def _write_checkpoint(self, end_exec_time):
		try:
			os.makedirs(os.path.dirname(self.timestamp_file), exist_ok=True)
			temporary_file = "%s.tmp" % self.timestamp_file
			with open(temporary_file, "w", encoding="ascii") as checkpoint:
				checkpoint.write(end_exec_time.isoformat() + "Z\n")
			os.replace(temporary_file, self.timestamp_file)
		except OSError as error:
			return S_ERROR("Could not persist GreenDigit timestamp: %s" % error)
		return S_OK()

	def _post_job(self, payload):
		"""Submit one job record to the collector."""
		if not self.collector_api_key:
			return S_ERROR("CollectorAPIKey is not configured")
		if not self.collector_ca_path:
			return S_ERROR(
				"IGTF CA bundle is not configured; set CollectorCAPath or X509_CERT_DIR"
			)

		url = "%s/jobs" % self.collector_base_url.rstrip("/")
		try:
			response = requests.post(
				url,
				headers={"X-API-Key": self.collector_api_key},
				json=payload,
				timeout=10,
				verify=self.collector_ca_path,
				allow_redirects=False,
			)
		except requests.RequestException as error:
			self.log.exception("Failed to submit GreenDigit job")
			return S_ERROR(str(error))

		if response.status_code != 202:
			message = "Collector job submission failed with HTTP %s: %s" % (
				response.status_code, response.text
			)
			self.log.error(message)
			return S_ERROR(message)
		return S_OK(response.text)

	def _report_completed_jobs(self, since, until):
		"""Fetch completed jobs in timestamp order and submit them individually."""
		job_result = self.job_monitoring_client.getJobs(
			{"Status": JobStatus.JOB_FINAL_STATES},
			cutDate=since,
		)
		if not job_result["OK"]:
			return job_result
		job_ids = job_result["Value"]
		if not job_ids:
			return S_OK()

		attribute_result = self.job_monitoring_client.getJobsSummary(job_ids)
		if not attribute_result["OK"]:
			return attribute_result
		attributes_by_job = attribute_result["Value"]

		jobs_to_report = []
		for job_id in job_ids:
			attributes = attributes_by_job.get(int(job_id), {})
			try:
				end_exec_time = self._parse_timestamp(attributes.get("EndExecTime"))
			except (TypeError, ValueError) as error:
				return S_ERROR("Job %s has invalid EndExecTime: %s" % (job_id, error))
			if since < end_exec_time <= until:
				jobs_to_report.append((end_exec_time, int(job_id), attributes))
		jobs_to_report.sort(key=lambda job: (job[0], job[1]))
		if not jobs_to_report:
			return S_OK()

		job_ids = [job_id for _, job_id, _ in jobs_to_report]
		parameter_result = self.job_monitoring_client.getJobParameters(job_ids)
		if not parameter_result["OK"]:
			return parameter_result
		parameters_by_job = parameter_result["Value"]

		for end_exec_time, job_id, attributes in jobs_to_report:
			payload_result = self.build_payload(
				job_id,
				attributes,
				parameters_by_job.get(int(job_id), {}),
				self.collector_job_fields,
			)
			if not payload_result["OK"]:
				return payload_result
			if self.dry_run:
				self.log.verbose(
					"Dry-run payload for job %s:\n%s"
					% (job_id, json.dumps(payload_result["Value"], indent=2, sort_keys=True))
				)
				continue

			submission_result = self._post_job(payload_result["Value"])
			if not submission_result["OK"]:
				return submission_result

			checkpoint_result = self._write_checkpoint(end_exec_time)
			if not checkpoint_result["OK"]:
				return checkpoint_result

		return S_OK()

	def execute(self):
		"""Run one polling cycle."""
		if (
			self.collector_job_fields is None
			or self.cycles_since_fields_refresh >= self.collector_fields_refresh_cycles
		):
			result = self.get_collector_job_fields()
			if not result["OK"]:
				return result
			self.collector_job_fields = result["Value"]
			self.cycles_since_fields_refresh = 0

		self.cycles_since_fields_refresh += 1
		if not self.dry_run and not self.collector_api_key:
			return S_ERROR("CollectorAPIKey is not configured")
		if not self.collector_ca_path:
			return S_ERROR(
				"IGTF CA bundle is not configured; set CollectorCAPath or X509_CERT_DIR"
			)

		now = datetime.now(timezone.utc).replace(tzinfo=None)
		since = self._read_checkpoint(now)
		if isinstance(since, dict) and not since["OK"]:
			return since
		return self._report_completed_jobs(since, now)
