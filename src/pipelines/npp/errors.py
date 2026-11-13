"""NPP failure types.

They are kept distinct so an ingestion run's ``error_message`` states clearly
*what* went wrong:

* :class:`NppApiError`          - the endpoint returned an HTTP error;
* :class:`EmptyResponseError`   - the endpoint answered ``[]``;
* :class:`InvalidResponseError` - the response is not the expected structure
  (not JSON, not a list, missing or extra fields, unexpected series), which
  most likely means the undocumented endpoint changed.

Individual invalid *records* are not errors: they are rejected and recorded in
``pi.data_quality_issue`` while the valid records load.
"""

from __future__ import annotations

from src.common.exceptions import IngestionError


class NppError(IngestionError):
    """Base class for NPP failures."""


class NppApiError(NppError):
    """The endpoint returned an HTTP error status."""


class EmptyResponseError(NppError):
    """The endpoint returned an empty list.

    NPP answers ``[]`` (HTTP 200) for days without data, for future dates and
    for malformed dates alike, so an empty response is never treated as success.
    """


class InvalidResponseError(NppError):
    """The response is not structurally what the pipeline was built for."""
