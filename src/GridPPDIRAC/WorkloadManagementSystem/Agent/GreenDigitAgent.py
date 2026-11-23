"""Report completed DIRAC jobs to the GreenDigit collector.

Jobs and their data are read through JobMonitoringClient and mapped to the
collector's accepted fields. Jobs are selected, ordered and checkpointed by
LastUpdateTime, so a job whose EndExecTime is older than the checkpoint is still
reported. EndExecTime is only reported; final-state jobs without one are skipped.
Dry-run mode logs payloads without submitting them or advancing the checkpoint.

Note: Payload fields are determined solely by the static source maps. New or
renamed collector fields require updating those maps and redeploying the agent.

Submission recovery: HTTP 202 advances the LastUpdateTime checkpoint. HTTP 401 and
503 end the current batch. HTTP 422 stops the agent because the static payload was
rejected. Same-timestamp siblings may be excluded in the next cycle after an
earlier accepted sibling advances the checkpoint and a later sibling is rejected.
"""

from datetime import datetime, timedelta, timezone
import json
import os
from urllib.parse import urljoin

import requests

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Security import Locations
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import (
    JobMonitoringClient,
)
from DIRAC.WorkloadManagementSystem.Client import JobStatus


# Field names match their DIRAC source names in JobParametersDB and JobDB.
PARAMETER_FIELDS = (
    "ModelName",
    "CPUNormalizationFactor",
    "HostName",
    "JobType",
    "LoadAverage",
    "MemoryUsed(kb)",
    "NormCPUTime(s)",
    "ScaledCPUTime(s)",
    "TotalCPUTime(s)",
    "WallClockTime(s)",
    "DiskSpace(MB)",
    "CEQueue",
    "GridCE",
)

ATTRIBUTE_FIELDS = (
    "JobGroup",
    "JobName",
    "OwnerGroup",
    "RescheduleCounter",
    "Site",
    "Status",
    "SubmissionTime",
    "StartExecTime",
    "EndExecTime",
)

TIMESTAMP_FIELDS = frozenset({"SubmissionTime", "StartExecTime", "EndExecTime"})


class GreenDigitAgent(AgentModule):
    """Periodically report completed DIRAC jobs to the GreenDigit collector."""

    def initialize(self):
        """Initialize agent state and configuration."""
        self.collector_base_url = self.am_getOption(
            "CollectorBaseURL", "https://dirac.egi.eu/job-reporting"
        ).rstrip("/")
        self.collector_api_key = self.am_getOption("CollectorAPIKey", "")
        self.dry_run = self.am_getOption("DryRun", False)
        self.collector_ca_path = self.am_getOption(
            "CollectorCAPath", Locations.getCAsLocation()
        )
        if not self.collector_ca_path:
            message = "IGTF CA bundle is not configured; configure DIRAC CA locations or CollectorCAPath"
            self.log.error(message)
            return S_ERROR(message)
        if not self.dry_run and not self.collector_api_key:
            message = "CollectorAPIKey is required unless DryRun is enabled"
            self.log.error(message)
            return S_ERROR(message)
        self.timestamp_file = os.path.join(
            self.am_getWorkDirectory(), "green_digit_last_end_time"
        )
        self.job_monitoring_client = JobMonitoringClient()
        self.log.info(
            "============================================================================"
        )
        self.log.info(
            "GreenDigitAgent initialized: completed-job reporting via JobMonitoringClient"
        )
        self.log.info("CollectorBaseURL: %s" % self.collector_base_url)
        self.log.info("PollingTime: %s seconds" % self.am_getOption("PollingTime"))
        self.log.info("DryRun: %s" % self.dry_run)
        self.log.info(
            "CollectorCAPath: %s" % (self.collector_ca_path or "not configured")
        )
        self.log.info(
            "CollectorAPIKey: %s"
            % ("configured" if self.collector_api_key else "not configured")
        )
        self.log.info("Checkpoint file: %s" % self.timestamp_file)
        if self.dry_run:
            self.log.info(
                "Dry run: payloads logged at VERBOSE; no POSTs or checkpoint updates"
            )
        else:
            self.log.info(
                "Live reporting: checkpoint advances only after HTTP 202 acceptance"
            )
        return S_OK()

    def _collector_url(self, endpoint):
        """Join an API endpoint to the configured collector base URL."""
        return urljoin("%s/" % self.collector_base_url, endpoint.lstrip("/"))

    def get_collector_health(self):
        """Return the collector health response."""
        url = self._collector_url("health")
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
                response.status_code,
                response.text,
            )
            self.log.error(message)
            return S_ERROR(message)

        try:
            health = response.json()
        except ValueError:
            health = response.text
        return S_OK(health)

    @classmethod
    def _json_value(cls, value, is_timestamp=False, timespec="auto"):
        """Convert DIRAC values to JSON-compatible values, formatting known timestamps."""
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        if is_timestamp:
            value = cls._parse_timestamp(value)
            return value.isoformat(timespec=timespec) + "Z"
        return value

    @classmethod
    def build_payload(cls, job_id, attributes, parameters):
        """Build a collector payload from the static field lists."""
        job_parameters = {"JobID": int(job_id)}
        for field_name in PARAMETER_FIELDS:
            value = parameters.get(field_name)
            if value is not None:
                try:
                    job_parameters[field_name] = cls._json_value(
                        value,
                        is_timestamp=field_name in TIMESTAMP_FIELDS,
                    )
                except (TypeError, ValueError) as error:
                    return S_ERROR(
                        "Invalid timestamp for job parameter %s: %s"
                        % (field_name, error)
                    )

        job_attributes = {}
        for field_name in ATTRIBUTE_FIELDS:
            value = attributes.get(field_name)
            if value is not None:
                try:
                    job_attributes[field_name] = cls._json_value(
                        value,
                        is_timestamp=field_name in TIMESTAMP_FIELDS,
                        timespec="seconds"
                        if field_name in TIMESTAMP_FIELDS
                        else "auto",
                    )
                except (TypeError, ValueError) as error:
                    return S_ERROR(
                        "Invalid timestamp for job attribute %s: %s"
                        % (field_name, error)
                    )

        return S_OK(
            {"job_parameters": job_parameters, "job_attributes": job_attributes}
        )

    @staticmethod
    def _parse_timestamp(value):
        """Parse a checkpoint or monitoring-client timestamp as naive UTC."""
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise ValueError("Timestamp is not a datetime")
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

    def _write_checkpoint(self, last_update_time):
        try:
            os.makedirs(os.path.dirname(self.timestamp_file), exist_ok=True)
            temporary_file = "%s.tmp" % self.timestamp_file
            with open(temporary_file, "w", encoding="ascii") as checkpoint:
                checkpoint.write(last_update_time.isoformat() + "Z\n")
            os.replace(temporary_file, self.timestamp_file)
        except OSError as error:
            return S_ERROR("Could not persist GreenDigit timestamp: %s" % error)
        return S_OK()

    def _post_job(self, payload):
        """Submit one job record to the collector."""
        url = self._collector_url("jobs")
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
                response.status_code,
                response.text,
            )
            self.log.error(message)
            result = S_ERROR(message)
            result["StatusCode"] = response.status_code
            if response.status_code == 401:
                self.log.error(
                    "Collector authentication failed; batch will retry next cycle"
                )
            elif response.status_code == 503:
                self.log.error(
                    "Collector returned HTTP 503; job will be retried next cycle"
                )
            return result
        return S_OK(response.text)

    def _report_completed_jobs(self, since, until):
        """Fetch completed jobs in LastUpdateTime order and submit them individually."""
        job_result = self.job_monitoring_client.getJobs(
            {"Status": JobStatus.JOB_FINAL_STATES},
            cutDate=since,
        )
        if not job_result["OK"]:
            return job_result
        job_ids = job_result["Value"]
        self.log.info("Monitoring returned %s final-job candidates" % len(job_ids))
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
                last_update_time = self._parse_timestamp(
                    attributes.get("LastUpdateTime")
                )
            except (TypeError, ValueError) as error:
                self.log.warn(
                    "Skipping final state job %s with invalid LastUpdateTime: %s"
                    % (job_id, error)
                )
                continue
            if not since < last_update_time <= until:
                continue
            if attributes.get("EndExecTime") is None:
                self.log.warn(
                    "Skipping final state job %s with no EndExecTime (status=%s)"
                    % (job_id, attributes.get("Status", "unknown"))
                )
                continue
            jobs_to_report.append((last_update_time, int(job_id), attributes))
        jobs_to_report.sort(key=lambda job: (job[0], job[1]))
        self.log.info("Jobs in LastUpdateTime window: %s" % len(jobs_to_report))
        if not jobs_to_report:
            return S_OK()

        job_ids = [job_id for _, job_id, _ in jobs_to_report]
        parameter_result = self.job_monitoring_client.getJobParameters(job_ids)
        if not parameter_result["OK"]:
            return parameter_result
        parameters_by_job = parameter_result["Value"]

        for last_update_time, job_id, attributes in jobs_to_report:
            parameters = parameters_by_job.get(int(job_id), {})
            payload_result = self.build_payload(job_id, attributes, parameters)
            if not payload_result["OK"]:
                return payload_result
            self.log.verbose(
                "Payload for job %s:\n%s"
                % (
                    job_id,
                    json.dumps(payload_result["Value"], indent=2, sort_keys=True),
                )
            )
            if self.dry_run:
                continue

            submission_result = self._post_job(payload_result["Value"])
            if (
                not submission_result["OK"]
                and submission_result.get("StatusCode") == 422
            ):
                self.log.error(
                    "Stopping GreenDigitAgent because collector rejected the payload"
                    " (client-side bug)"
                )
                self.am_createStopAgentFile()
            # 401 and 503 end this batch; only the last accepted timestamp is checkpointed.
            if not submission_result["OK"]:
                return submission_result

            checkpoint_result = self._write_checkpoint(last_update_time)
            if not checkpoint_result["OK"]:
                return checkpoint_result

        self.log.info(
            "%s %s jobs; checkpoint %s"
            % (
                "Dry-run built payloads for" if self.dry_run else "Collector accepted",
                len(jobs_to_report),
                "unchanged" if self.dry_run else "updated",
            )
        )
        return S_OK()

    def execute(self):
        """Run one polling cycle."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        since = self._read_checkpoint(now)
        if isinstance(since, dict) and not since["OK"]:
            return since
        self.log.info(
            "LastUpdateTime window (UTC): (%s, %s]"
            % (since.isoformat(), now.isoformat())
        )
        return self._report_completed_jobs(since, now)
