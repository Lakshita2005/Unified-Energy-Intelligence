"""IMD (India Meteorological Department) ingestion: AWS/ARG observations and city forecasts (Phase 2).

Built from the official API reference (https://api.imd.gov.in/public/api_reference.html). Live IMD access
is not yet authorised: everything here has been exercised with mocked HTTP only, and the response
structure, units, timestamps and identifiers are UNVERIFIED until authorised live testing.
"""
