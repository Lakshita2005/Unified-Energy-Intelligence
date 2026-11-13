"""Tests for src.ingestion.validation."""

from __future__ import annotations

from decimal import Decimal

from src.ingestion.models import ParsedRecord, Severity, ValidationIssue
from src.ingestion.validation import (
    AllowedValues,
    CompositeValidator,
    FunctionValidator,
    NumericRange,
    RequiredFields,
    validate_records,
)


def _record(**values: object) -> ParsedRecord:
    return ParsedRecord(key="k", values=dict(values), raw=dict(values))


def test_required_fields() -> None:
    issues = RequiredFields("a", "b", "c").validate(_record(a=1, b="  ", c=None))

    assert [i.field for i in issues] == ["b", "c"]
    assert all(i.code == "MISSING_REQUIRED_FIELD" and i.severity is Severity.ERROR for i in issues)


def test_numeric_range_bounds_and_types() -> None:
    rule = NumericRange("p", minimum=0, maximum=100)

    assert rule.validate(_record(p=Decimal("50"))) == []
    assert rule.validate(_record(p=None)) == []
    assert rule.validate(_record(p=-1))[0].code == "OUT_OF_RANGE"
    assert rule.validate(_record(p=101))[0].code == "OUT_OF_RANGE"
    assert rule.validate(_record(p="12"))[0].code == "INVALID_TYPE"
    assert rule.validate(_record(p=True))[0].code == "INVALID_TYPE"


def test_warning_severity_is_respected() -> None:
    issue = NumericRange("p", maximum=10, severity=Severity.WARNING).validate(_record(p=11))[0]

    assert issue.severity is Severity.WARNING


def test_allowed_values() -> None:
    rule = AllowedValues("m", {"DAM", "RTM"})

    assert rule.validate(_record(m="DAM")) == []
    assert rule.validate(_record(m="XYZ"))[0].code == "INVALID_VALUE"


def test_validate_records_splits_by_severity() -> None:
    validator = CompositeValidator(
        RequiredFields("a"),
        FunctionValidator(lambda r: [ValidationIssue("ODD", "odd", Severity.WARNING)] if r.values["a"] % 2 else None),
    )
    records = [_record(a=2), _record(a=3), _record(a=None)]

    outcome = validate_records(records, CompositeValidator(RequiredFields("a")))
    assert len(outcome.accepted) == 2 and len(outcome.rejected) == 1

    outcome = validate_records(records[:2], validator)
    assert [len(issues) for _, issues in outcome.accepted] == [0, 1]
    assert outcome.rejected == []
