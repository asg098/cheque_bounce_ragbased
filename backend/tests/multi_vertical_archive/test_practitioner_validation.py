import pytest
import os
import sys

# Ensure backend root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from validation.practitioner_validation_framework import PractitionerValidationFramework

def test_practitioner_validation_framework_execution():
    report = PractitionerValidationFramework.run_blind_validation()
    assert report is not None
    assert "stage_identification_accuracy_pct" in report
    assert "disagreement_telemetry" in report
    assert report["total_matters_evaluated"] > 0
