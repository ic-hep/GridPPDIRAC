"""Live integration check for the GreenDigit collector health endpoint."""

import os

import pytest
import requests


COLLECTOR_BASE_URL = os.environ.get(
    "GREEN_DIGIT_COLLECTOR_BASE_URL",
    "https://dirac.egi.eu/job-reporting",
).rstrip("/")
CA_BUNDLE = os.environ.get("CA_BUNDLE") or os.environ.get("X509_CERT_DIR")


def test_collector_health():
    """Contact the configured collector using the IGTF CA bundle."""
    if not CA_BUNDLE:
        pytest.skip("Set CA_BUNDLE or X509_CERT_DIR to the IGTF CA bundle location")

    response = requests.get(
        "%s/health" % COLLECTOR_BASE_URL,
        timeout=10,
        verify=CA_BUNDLE,
        allow_redirects=False,
    )

    assert not response.is_redirect, (
        "Collector health endpoint redirected to %s"
        % response.headers.get("Location", "an unspecified location")
    )
    assert response.status_code == 200, response.text
    print("Collector health response: %s" % response.text)