"""Unit tests for GreenDigitAgent helpers."""

from datetime import datetime
from unittest.mock import Mock, call, patch

from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent import GreenDigitAgent


def test_get_collector_health():
    """Request and return collector health using the configured credentials."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.collector_base_url = "https://collector.example/job-reporting/"
    agent.collector_ca_path = "/test/igtf-cas"
    agent.log = Mock()

    response = Mock(status_code=200, text='{"status": "ok"}')
    response.json.return_value = {"status": "ok"}

    with patch(
        "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.requests.get",
        return_value=response,
    ) as get:
        result = agent.get_collector_health()

    assert result["OK"]
    assert result["Value"] == {"status": "ok"}
    get.assert_called_once_with(
        "https://collector.example/job-reporting/health",
        timeout=10,
        verify="/test/igtf-cas",
        allow_redirects=False,
    )


def test_get_collector_job_fields():
    """Fetch the collector's accepted field names without authentication."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.collector_base_url = "https://collector.example/job-reporting/"
    agent.collector_ca_path = "/test/igtf-cas"
    agent.log = Mock()

    fields = {"job_parameter_keys": ["JobID"], "job_attribute_keys": ["Status"]}
    response = Mock(status_code=200, text="{}")
    response.json.return_value = fields

    with patch(
        "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.requests.get",
        return_value=response,
    ) as get:
        result = agent.get_collector_job_fields()

    assert result["OK"]
    assert result["Value"] == fields
    get.assert_called_once_with(
        "https://collector.example/job-reporting/job-fields",
        timeout=10,
        verify="/test/igtf-cas",
        allow_redirects=False,
    )


def test_execute_refreshes_job_fields_every_configured_cycles():
    """Refresh fields on the first cycle, then after each configured interval."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.collector_job_fields = None
    agent.collector_fields_refresh_cycles = 2
    agent.cycles_since_fields_refresh = 0
    agent.collector_api_key = "test-api-key"
    agent.dry_run = False
    agent.collector_ca_path = "/test/igtf-cas"
    agent.log = Mock()
    agent.get_collector_job_fields = Mock(return_value=S_OK({"job_parameter_keys": [],
                                                              "job_attribute_keys": []}))
    agent._read_checkpoint = Mock(return_value=datetime(2026, 11, 6, 0, 0, 0))
    agent._report_completed_jobs = Mock(return_value=S_OK())

    assert agent.execute()["OK"]
    assert agent.execute()["OK"]
    assert agent.get_collector_job_fields.call_count == 1
    assert agent.execute()["OK"]
    assert agent.get_collector_job_fields.call_count == 2
    assert agent._report_completed_jobs.call_count == 3


def test_build_payload_maps_only_available_collector_fields():
    """Build the collector payload from explicit DIRAC field mappings."""
    attributes = {
        "OwnerGroup": "analysis",
        "Status": "Done",
        "EndExecTime": datetime(2026, 11, 6, 14, 30, 0, 987654),
        "Site": None,
    }
    parameters = {
        "HostName": "worker.example.org",
        "JobType": "User",
        "UnknownParameter": "do not forward",
    }
    collector_fields = {
        "job_parameter_keys": ["JobID", "HostName", "JobType", "UnknownParameter"],
        "job_attribute_keys": ["OwnerGroup", "Status", "EndExecTime", "Site"],
    }

    payload_result = GreenDigitAgent.build_payload(123, attributes, parameters, collector_fields)

    assert payload_result["OK"]
    assert payload_result["Value"] == {
        "job_parameters": {
            "JobID": 123,
            "HostName": "worker.example.org",
            "JobType": "User",
        },
        "job_attributes": {
            "OwnerGroup": "analysis",
            "Status": "Done",
            "EndExecTime": "2026-11-06T14:30:00Z",
        },
    }


def test_job_attribute_timestamp_uses_utc_second_precision():
    """Format collector job attribute timestamps in UTC to whole seconds."""
    payload_result = GreenDigitAgent.build_payload(
        123,
        {"EndExecTime": "2026-11-06T14:30:00.987654"},
        {},
        {"job_parameter_keys": ["JobID"], "job_attribute_keys": ["EndExecTime"]},
    )

    assert payload_result["OK"]
    assert payload_result["Value"]["job_attributes"]["EndExecTime"] == "2026-11-06T14:30:00Z"


def test_json_value_normalizes_timestamp_strings_and_preserves_microseconds():
    """Treat naive timestamps as UTC and retain sub-second precision."""
    naive_timestamp = datetime(2026, 11, 6, 14, 30, 0, 123456)
    timestamp_string = "2026-11-06T14:30:00.123456"

    assert GreenDigitAgent._json_value(naive_timestamp) == "2026-11-06T14:30:00.123456Z"
    assert GreenDigitAgent._json_value(timestamp_string) == "2026-11-06T14:30:00.123456Z"


def test_report_completed_jobs_queries_and_checkpoints_after_each_post():
    """Query through JobMonitoringClient and checkpoint accepted jobs in order."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([11, 10])
    attributes = {
        10: {"VO": "vo", "EndExecTime": datetime(2026, 11, 6, 14, 30)},
        11: {"VO": "vo", "EndExecTime": datetime(2026, 11, 6, 14, 31)},
    }
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(attributes)
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({10: {}, 11: {}})
    agent.collector_job_fields = {
        "job_parameter_keys": ["JobID"],
        "job_attribute_keys": ["EndExecTime"],
    }
    agent.dry_run = False
    agent.log = Mock()
    agent.build_payload = Mock(side_effect=lambda job_id, *_: S_OK({"job_id": job_id}))
    agent._post_job = Mock(side_effect=[S_OK("accepted"), S_OK("accepted")])
    agent._write_checkpoint = Mock(return_value=S_OK())
    since = datetime(2026, 11, 6, 14, 0)
    until = datetime(2026, 11, 6, 15, 0)

    result = agent._report_completed_jobs(since, until)

    assert result["OK"]
    agent.job_monitoring_client.getJobs.assert_called_once_with(
        {"Status": JobStatus.JOB_FINAL_STATES},
        cutDate=since,
    )
    agent.job_monitoring_client.getJobsSummary.assert_called_once_with([11, 10])
    assert agent._post_job.call_args_list == [call({"job_id": 10}), call({"job_id": 11})]
    agent.job_monitoring_client.getJobParameters.assert_called_once_with([10, 11])
    assert agent._write_checkpoint.call_args_list == [
        call(datetime(2026, 11, 6, 14, 30)),
        call(datetime(2026, 11, 6, 14, 31)),
    ]


def test_report_completed_jobs_stops_without_checkpoint_after_failed_post():
    """Keep the cursor at the last accepted job when collector submission fails."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([10, 11])
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(
        {
            10: {"VO": "vo", "EndExecTime": datetime(2026, 11, 6, 14, 30)},
            11: {"VO": "vo", "EndExecTime": datetime(2026, 11, 6, 14, 31)},
        }
    )
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({10: {}, 11: {}})
    agent.collector_job_fields = {
        "job_parameter_keys": ["JobID"],
        "job_attribute_keys": ["EndExecTime"],
    }
    agent.dry_run = False
    agent.log = Mock()
    agent.build_payload = Mock(side_effect=lambda job_id, *_: S_OK({"job_id": job_id}))
    agent._post_job = Mock(side_effect=[S_OK("accepted"), {"OK": False, "Message": "HTTP 503"}])
    agent._write_checkpoint = Mock(return_value=S_OK())

    result = agent._report_completed_jobs(datetime(2026, 11, 6), datetime(2026, 11, 7))

    assert not result["OK"]
    assert agent._post_job.call_count == 2
    agent._write_checkpoint.assert_called_once_with(datetime(2026, 11, 6, 14, 30))


def test_report_completed_jobs_dry_run_logs_payload_without_post_or_checkpoint():
    """Dry-run logs payload JSON but neither submits nor advances the cursor."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([10])
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(
        {10: {"EndExecTime": datetime(2026, 11, 6, 14, 30)}}
    )
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({10: {"HostName": "worker"}})
    agent.collector_job_fields = {
        "job_parameter_keys": ["JobID", "HostName"],
        "job_attribute_keys": ["EndExecTime"],
    }
    agent.dry_run = True
    agent.log = Mock()
    agent._post_job = Mock()
    agent._write_checkpoint = Mock()

    result = agent._report_completed_jobs(datetime(2026, 11, 6), datetime(2026, 11, 7))

    assert result["OK"]
    agent.log.verbose.assert_called_once()
    logged_message = agent.log.verbose.call_args.args[0]
    assert '"HostName": "worker"' in logged_message
    assert '"EndExecTime": "2026-11-06T14:30:00Z"' in logged_message
    agent._post_job.assert_not_called()
    agent._write_checkpoint.assert_not_called()