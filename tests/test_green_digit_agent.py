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


def test_initialize_uses_dirac_ca_location_by_default():
    """Use DIRAC's configured CA path when no collector-specific path is set."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.am_setOption = Mock()
    agent.am_getOption = Mock(
        side_effect=lambda name, default=None: "test-api-key" if name == "CollectorAPIKey" else default
    )
    agent.am_getWorkDirectory = Mock(return_value="/tmp/green-digit")
    agent.log = Mock()

    with (
        patch(
            "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.Locations.getCAsLocation",
            return_value="/dirac/ca-bundle",
        ),
        patch(
            "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.JobMonitoringClient"
        ),
    ):
        result = agent.initialize()

    assert result["OK"]
    assert agent.collector_ca_path == "/dirac/ca-bundle"


def test_initialize_requires_api_key_for_live_mode():
    """Reject live-mode configuration without an API key during startup."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.am_setOption = Mock()
    agent.am_getOption = Mock(side_effect=lambda _name, default=None: default)
    agent.log = Mock()

    with patch(
        "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.Locations.getCAsLocation",
        return_value="/dirac/ca-bundle",
    ):
        result = agent.initialize()

    assert not result["OK"]
    assert result["Message"] == "CollectorAPIKey is required unless DryRun is enabled"
    agent.log.error.assert_called_once_with(result["Message"])


def test_initialize_allows_missing_api_key_in_dry_run():
    """Allow dry-run startup without collector credentials."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.am_setOption = Mock()
    agent.am_getOption = Mock(
        side_effect=lambda name, default=None: True if name == "DryRun" else default
    )
    agent.am_getWorkDirectory = Mock(return_value="/tmp/green-digit")
    agent.log = Mock()

    with (
        patch(
            "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.Locations.getCAsLocation",
            return_value="/dirac/ca-bundle",
        ),
        patch(
            "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.JobMonitoringClient"
        ),
    ):
        result = agent.initialize()

    assert result["OK"]
    assert agent.dry_run
    assert not agent.collector_api_key


def test_initialize_fails_when_no_ca_location_is_configured():
    """Fail startup when neither a collector CA path nor DIRAC CAs exist."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.am_setOption = Mock()
    agent.am_getOption = Mock(side_effect=lambda _name, default=None: default)
    agent.log = Mock()

    with (
        patch(
            "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.Locations.getCAsLocation",
            return_value=None,
        ),
        patch(
            "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.JobMonitoringClient"
        ) as job_monitoring_client,
    ):
        result = agent.initialize()

    assert not result["OK"]
    assert "IGTF CA bundle is not configured" in result["Message"]
    agent.log.error.assert_called_once_with(result["Message"])
    job_monitoring_client.assert_not_called()


def test_post_job_returns_unauthorized_without_stopping_agent():
    """Return 401 so the reporting loop can retry it in the next cycle."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.collector_api_key = "invalid-api-key"
    agent.collector_ca_path = "/test/igtf-cas"
    agent.collector_base_url = "https://collector.example/job-reporting"
    agent.log = Mock()
    agent.am_createStopAgentFile = Mock()
    response = Mock(status_code=401, text="Unauthorized")

    with patch(
        "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.requests.post",
        return_value=response,
    ) as post:
        result = agent._post_job({"job_id": 123})

    assert not result["OK"]
    assert "HTTP 401" in result["Message"]
    agent.am_createStopAgentFile.assert_not_called()
    post.assert_called_once_with(
        "https://collector.example/job-reporting/jobs",
        headers={"X-API-Key": "invalid-api-key"},
        json={"job_id": 123},
        timeout=10,
        verify="/test/igtf-cas",
        allow_redirects=False,
    )


def test_post_job_returns_status_for_unprocessable_payload():
    """Return the 422 status for the reporting loop to stop on."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.collector_api_key = "test-api-key"
    agent.collector_ca_path = "/test/igtf-cas"
    agent.collector_base_url = "https://collector.example/job-reporting"
    agent.log = Mock()
    agent.am_createStopAgentFile = Mock()
    response = Mock(status_code=422, text="Invalid job payload")

    with patch(
        "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.requests.post",
        return_value=response,
    ):
        result = agent._post_job({"job_id": 123})

    assert not result["OK"]
    assert "HTTP 422" in result["Message"]
    assert result["StatusCode"] == 422
    agent.am_createStopAgentFile.assert_not_called()


def test_build_payload_uses_static_maps_for_available_source_values():
    """Build payload from static maps without consulting collector metadata."""
    attributes = {
        "Owner": "example-user",
        "OwnerDN": "/CN=Example User/O=Example Org",
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
    payload_result = GreenDigitAgent.build_payload(123, attributes, parameters)

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
    )

    assert payload_result["OK"]
    assert payload_result["Value"]["job_attributes"]["EndExecTime"] == "2026-11-06T14:30:00Z"


def test_json_value_normalizes_timestamp_strings_and_preserves_microseconds():
    """Treat naive timestamps as UTC and retain sub-second precision."""
    naive_timestamp = datetime(2026, 11, 6, 14, 30, 0, 123456)
    timestamp_string = "2026-11-06T14:30:00.123456"
    offset_timestamp = datetime.fromisoformat("2026-11-06T16:30:00.123456+02:00")

    assert GreenDigitAgent._json_value(naive_timestamp, is_timestamp=True) == "2026-11-06T14:30:00.123456Z"
    assert GreenDigitAgent._json_value(timestamp_string, is_timestamp=True) == "2026-11-06T14:30:00.123456Z"
    assert GreenDigitAgent._json_value(offset_timestamp, is_timestamp=True) == "2026-11-06T14:30:00.123456Z"


def test_build_payload_reports_invalid_value_for_timestamp_field():
    """Reject malformed values for known timestamp fields with field context."""
    result = GreenDigitAgent.build_payload(
        123,
        {"EndExecTime": "not-a-date"},
        {},
    )

    assert not result["OK"]
    assert "Invalid timestamp for job attribute EndExecTime" in result["Message"]


def test_report_completed_jobs_queries_and_checkpoints_each_accepted_job():
    """Query through JobMonitoringClient and checkpoint accepted timestamps."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([11, 10])
    attributes = {
        10: {"VO": "vo", "EndExecTime": datetime(2026, 11, 6, 14, 30)},
        11: {"VO": "vo", "EndExecTime": datetime(2026, 11, 6, 14, 31)},
    }
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(attributes)
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({10: {}, 11: {}})
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


def test_report_completed_jobs_skips_missing_end_exec_time_and_continues():
    """Skip final jobs without an execution end time without blocking others."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([10, 11])
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(
        {
            10: {"Status": "Killed", "EndExecTime": None},
            11: {"Status": "Done", "EndExecTime": datetime(2026, 11, 6, 14, 31)},
        }
    )
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({11: {}})
    agent.dry_run = False
    agent.log = Mock()
    agent._post_job = Mock(return_value=S_OK("accepted"))
    agent._write_checkpoint = Mock(return_value=S_OK())

    result = agent._report_completed_jobs(datetime(2026, 11, 6), datetime(2026, 11, 7))

    assert result["OK"]
    agent.log.warn.assert_called_once_with(
        "Skipping final state job 10 with no EndExecTime (status=Killed)"
    )
    agent.job_monitoring_client.getJobParameters.assert_called_once_with([11])
    agent._post_job.assert_called_once()
    agent._write_checkpoint.assert_called_once_with(datetime(2026, 11, 6, 14, 31))


def test_report_completed_jobs_skips_invalid_end_exec_time_and_continues():
    """Skip an unparseable timestamp while continuing with valid jobs."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([10, 11])
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(
        {
            10: {"Status": "Done", "EndExecTime": "not-a-date"},
            11: {"Status": "Done", "EndExecTime": datetime(2026, 11, 6, 14, 31)},
        }
    )
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({11: {}})
    agent.dry_run = False
    agent.log = Mock()
    agent._post_job = Mock(return_value=S_OK("accepted"))
    agent._write_checkpoint = Mock(return_value=S_OK())

    result = agent._report_completed_jobs(datetime(2026, 11, 6), datetime(2026, 11, 7))

    assert result["OK"]
    warning = agent.log.warn.call_args.args[0]
    assert warning.startswith("Skipping final state job 10 with invalid EndExecTime:")
    agent.job_monitoring_client.getJobParameters.assert_called_once_with([11])
    agent._post_job.assert_called_once()
    agent._write_checkpoint.assert_called_once_with(datetime(2026, 11, 6, 14, 31))


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
    agent.dry_run = False
    agent.log = Mock()
    agent.build_payload = Mock(side_effect=lambda job_id, *_: S_OK({"job_id": job_id}))
    agent._post_job = Mock(side_effect=[S_OK("accepted"), {"OK": False, "Message": "HTTP 503"}])
    agent._write_checkpoint = Mock(return_value=S_OK())

    result = agent._report_completed_jobs(datetime(2026, 11, 6), datetime(2026, 11, 7))

    assert not result["OK"]
    assert agent._post_job.call_count == 2
    agent._write_checkpoint.assert_called_once_with(datetime(2026, 11, 6, 14, 30))


def test_report_completed_jobs_retries_after_collector_503():
    """Retry the same job next cycle after a transient collector 503."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([10])
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(
        {10: {"EndExecTime": datetime(2026, 11, 6, 14, 30)}}
    )
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({10: {}})
    agent.collector_api_key = "test-api-key"
    agent.collector_ca_path = "/test/igtf-cas"
    agent.collector_base_url = "https://collector.example/job-reporting"
    agent.dry_run = False
    agent.log = Mock()
    agent.am_createStopAgentFile = Mock()
    agent._write_checkpoint = Mock(return_value=S_OK())
    responses = [Mock(status_code=503, text="Temporarily unavailable"), Mock(status_code=202, text="accepted")]
    since = datetime(2026, 11, 6, 14, 0)
    until = datetime(2026, 11, 6, 15, 0)

    with patch(
        "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.requests.post",
        side_effect=responses,
    ) as post:
        first_result = agent._report_completed_jobs(since, until)
        agent._write_checkpoint.assert_not_called()

        retry_result = agent._report_completed_jobs(since, until)

    assert not first_result["OK"]
    assert retry_result["OK"]
    assert post.call_count == 2
    agent.am_createStopAgentFile.assert_not_called()
    agent._write_checkpoint.assert_called_once_with(datetime(2026, 11, 6, 14, 30))


def test_report_completed_jobs_skips_same_timestamp_siblings_after_401():
    """Advance on accepted job; timestamp filtering then omits both siblings."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([10, 11])
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(
        {
            10: {"EndExecTime": datetime(2026, 11, 6, 14, 30)},
            11: {"EndExecTime": datetime(2026, 11, 6, 14, 30)},
        }
    )
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({10: {}, 11: {}})
    agent.collector_api_key = "test-api-key"
    agent.collector_ca_path = "/test/igtf-cas"
    agent.collector_base_url = "https://collector.example/job-reporting"
    agent.dry_run = False
    agent.log = Mock()
    agent.am_createStopAgentFile = Mock()
    agent._write_checkpoint = Mock(return_value=S_OK())
    responses = [
        Mock(status_code=202, text="accepted"),
        Mock(status_code=401, text="Unauthorized"),
    ]
    since = datetime(2026, 11, 6, 14, 0)
    until = datetime(2026, 11, 6, 15, 0)

    with patch(
        "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.requests.post",
        side_effect=responses,
    ) as post:
        first_result = agent._report_completed_jobs(since, until)
        assert post.call_count == 2
        agent._write_checkpoint.assert_called_once_with(datetime(2026, 11, 6, 14, 30))

        retry_result = agent._report_completed_jobs(datetime(2026, 11, 6, 14, 30), until)

    assert not first_result["OK"]
    assert first_result["StatusCode"] == 401
    assert retry_result["OK"]
    assert post.call_count == 2
    agent.am_createStopAgentFile.assert_not_called()
    agent._write_checkpoint.assert_called_once_with(datetime(2026, 11, 6, 14, 30))


def test_report_completed_jobs_stops_on_422_without_refresh_or_retry():
    """Stop after a rejected static payload rather than refreshing/retrying."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([10])
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(
        {10: {"EndExecTime": datetime(2026, 11, 6, 14, 30)}}
    )
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({10: {}})
    agent.collector_api_key = "test-api-key"
    agent.collector_ca_path = "/test/igtf-cas"
    agent.collector_base_url = "https://collector.example/job-reporting"
    agent.dry_run = False
    agent.log = Mock()
    agent.am_createStopAgentFile = Mock()
    agent._write_checkpoint = Mock(return_value=S_OK())

    with patch(
        "GridPPDIRAC.WorkloadManagementSystem.Agent.GreenDigitAgent.requests.post",
        return_value=Mock(status_code=422, text="Invalid payload"),
    ) as post:
        result = agent._report_completed_jobs(datetime(2026, 11, 6), datetime(2026, 11, 7))

    assert not result["OK"]
    assert result["StatusCode"] == 422
    post.assert_called_once()
    agent.am_createStopAgentFile.assert_called_once_with()
    agent._write_checkpoint.assert_not_called()


def test_report_completed_jobs_dry_run_logs_payload_without_post_or_checkpoint():
    """Dry-run logs payload JSON but neither submits nor advances the cursor."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([10])
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(
        {10: {"EndExecTime": datetime(2026, 11, 6, 14, 30)}}
    )
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({10: {"HostName": "worker"}})
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


def test_report_completed_jobs_live_mode_logs_payload_before_post():
    """Log payload at VERBOSE in live mode before submitting it."""
    agent = GreenDigitAgent.__new__(GreenDigitAgent)
    agent.job_monitoring_client = Mock()
    agent.job_monitoring_client.getJobs.return_value = S_OK([10])
    agent.job_monitoring_client.getJobsSummary.return_value = S_OK(
        {10: {"EndExecTime": datetime(2026, 11, 6, 14, 30)}}
    )
    agent.job_monitoring_client.getJobParameters.return_value = S_OK({10: {"HostName": "worker"}})
    agent.dry_run = False
    agent.log = Mock()
    agent.build_payload = Mock(
        return_value=S_OK(
            {
                "job_parameters": {"JobID": 10, "HostName": "worker"},
                "job_attributes": {"EndExecTime": "2026-11-06T14:30:00Z"},
            }
        )
    )
    agent._post_job = Mock(return_value=S_OK("accepted"))
    agent._write_checkpoint = Mock(return_value=S_OK())

    result = agent._report_completed_jobs(datetime(2026, 11, 6), datetime(2026, 11, 7))

    assert result["OK"]
    agent.log.verbose.assert_called_once()
    assert '"HostName": "worker"' in agent.log.verbose.call_args.args[0]
    agent._post_job.assert_called_once()