"""IEX failure types.

They are kept distinct so an ingestion run's ``error_message`` states clearly
*what* went wrong:

* :class:`IexApiError`          - HTTP error, or an envelope with ``statusCode`` other than 200;
* :class:`EmptyResponseError`   - the API answered ``data: []``;
* :class:`InvalidResponseError` - the response is not the expected structure
  (not JSON, no envelope, missing or unexpected fields), which most likely
  means the undocumented API changed.

Individual invalid *records* are not errors: they are rejected and recorded in
``pi.data_quality_issue`` while the valid records load.
"""

from __future__ import annotations

from src.common.exceptions import IngestionError


class IexError(IngestionError):
    """Base class for IEX failures."""


class IexApiError(IexError):
    """The API returned an HTTP error or a non-200 ``statusCode``."""


class EmptyResponseError(IexError):
    """The API returned no records.

    IEX answers ``data: []`` (HTTP 200) for days not yet published and for days
    without data alike, so an empty response is never treated as success.
    """


class InvalidResponseError(IexError):
    """The response is not structurally what the pipeline was built for."""
