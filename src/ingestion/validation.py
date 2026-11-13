"""Standardised record validation.

A :class:`Validator` inspects one :class:`ParsedRecord` and returns zero or more
:class:`ValidationIssue` objects. ``ERROR`` issues reject the record; ``WARNING``
and ``INFO`` issues are recorded in ``pi.data_quality_issue`` but the record is
still loaded. Validators never modify records.

Compose the reusable rules below, or write custom ones::

    class MyPipeline(BaseIngestionPipeline):
        def validator(self) -> Validator:
            return CompositeValidator(
                RequiredFields("delivery_date", "time_block", "price_inr_per_mwh"),
                NumericRange("time_block", minimum=1, maximum=96),
                NumericRange("price_inr_per_mwh", maximum=10000, severity=Severity.WARNING),
            )
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Collection, Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from src.ingestion.models import ParsedRecord, Severity, ValidationIssue


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


class Validator(ABC):
    """Interface for record validators."""

    @abstractmethod
    def validate(self, record: ParsedRecord) -> list[ValidationIssue]:
        """Return the issues found in ``record`` (empty if valid)."""


class NoOpValidator(Validator):
    """Accepts every record."""

    def validate(self, record: ParsedRecord) -> list[ValidationIssue]:
        return []


class CompositeValidator(Validator):
    """Runs several validators and combines their issues."""

    def __init__(self, *validators: Validator) -> None:
        self.validators = validators

    def validate(self, record: ParsedRecord) -> list[ValidationIssue]:
        return [issue for validator in self.validators for issue in validator.validate(record)]


class RequiredFields(Validator):
    """Fields must be present and non-empty."""

    def __init__(self, *fields: str, severity: Severity = Severity.ERROR) -> None:
        self.fields = fields
        self.severity = severity

    def validate(self, record: ParsedRecord) -> list[ValidationIssue]:
        return [
            ValidationIssue("MISSING_REQUIRED_FIELD", f"Required field '{name}' is missing or empty",
                            self.severity, name)
            for name in self.fields
            if _is_missing(record.values.get(name))
        ]


class NumericRange(Validator):
    """A numeric field must lie within ``[minimum, maximum]`` (bounds optional).

    Missing values are ignored (combine with :class:`RequiredFields`); non-numeric
    values always produce an ERROR.
    """

    def __init__(
        self,
        field_name: str,
        *,
        minimum: float | Decimal | None = None,
        maximum: float | Decimal | None = None,
        severity: Severity = Severity.ERROR,
    ) -> None:
        self.field_name = field_name
        self.minimum = minimum
        self.maximum = maximum
        self.severity = severity

    def validate(self, record: ParsedRecord) -> list[ValidationIssue]:
        value = record.values.get(self.field_name)
        if _is_missing(value):
            return []
        if not _is_number(value):
            return [ValidationIssue("INVALID_TYPE", f"Field '{self.field_name}' must be numeric, got {value!r}",
                                    Severity.ERROR, self.field_name)]
        if self.minimum is not None and value < self.minimum:
            return [ValidationIssue("OUT_OF_RANGE", f"Field '{self.field_name}'={value} is below minimum {self.minimum}",
                                    self.severity, self.field_name)]
        if self.maximum is not None and value > self.maximum:
            return [ValidationIssue("OUT_OF_RANGE", f"Field '{self.field_name}'={value} is above maximum {self.maximum}",
                                    self.severity, self.field_name)]
        return []


class AllowedValues(Validator):
    """A field must take one of a fixed set of values (missing values are ignored)."""

    def __init__(self, field_name: str, allowed: Collection[Any], *, severity: Severity = Severity.ERROR) -> None:
        self.field_name = field_name
        self.allowed = frozenset(allowed)
        self.severity = severity

    def validate(self, record: ParsedRecord) -> list[ValidationIssue]:
        value = record.values.get(self.field_name)
        if _is_missing(value) or value in self.allowed:
            return []
        return [ValidationIssue("INVALID_VALUE", f"Field '{self.field_name}'={value!r} is not one of "
                                f"{sorted(map(str, self.allowed))}", self.severity, self.field_name)]


class FunctionValidator(Validator):
    """Wraps a function ``(record) -> list[ValidationIssue] | None`` for custom rules."""

    def __init__(self, func: Callable[[ParsedRecord], list[ValidationIssue] | None]) -> None:
        self.func = func

    def validate(self, record: ParsedRecord) -> list[ValidationIssue]:
        return list(self.func(record) or [])


@dataclass
class ValidationOutcome:
    """Records split by validation result.

    ``accepted`` pairs each loadable record with its non-blocking issues;
    ``rejected`` pairs each rejected record with all of its issues.
    """

    accepted: list[tuple[ParsedRecord, list[ValidationIssue]]] = field(default_factory=list)
    rejected: list[tuple[ParsedRecord, list[ValidationIssue]]] = field(default_factory=list)


def validate_records(records: Iterable[ParsedRecord], validator: Validator) -> ValidationOutcome:
    """Validate records, separating rejected ones (any ERROR issue) from accepted ones."""
    outcome = ValidationOutcome()
    for record in records:
        issues = validator.validate(record)
        if any(issue.severity is Severity.ERROR for issue in issues):
            outcome.rejected.append((record, issues))
        else:
            outcome.accepted.append((record, issues))
    return outcome
