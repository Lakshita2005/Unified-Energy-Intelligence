"""Open-Meteo failure types.

They are kept distinct so an ingestion run's ``error_message`` states clearly
*what* went wrong:

* :class:`OpenMeteoApiError`   - the API refused or failed the request;
* :class:`EmptyResponseError`  - the API answered but returned no hourly data;
* :class:`InvalidResponseError` - the API answered with a structurally invalid
  payload (bad JSON, missing series, wrong units/timezone, ...).

Individual invalid *records* are not errors: they are rejected and recorded in
``pi.data_quality_issue`` while the valid records load.
"""

from __future__ import annotations

from src.common.exceptions import IngestionError


class OpenMeteoError(IngestionError):
    """Base class for Open-Meteo failures."""


class OpenMeteoApiError(OpenMeteoError):
    """The API returned an error (e.g. HTTP 400 with a ``reason``)."""


class EmptyResponseError(OpenMeteoError):
    """The API returned a valid response with no hourly timestamps."""


class InvalidResponseError(OpenMeteoError):
    """The API response is not structurally valid."""
