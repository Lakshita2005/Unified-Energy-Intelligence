"""WBES reference values.

Region ids: the assessment documents ids 1-6 as ER, WR, NR, SR, NER and NLDC.
That mapping has **not** been verified against live WBES data (W2/W6), so
``REGION_MAPPING_VERIFIED`` is False and no PI ``region_code`` is assigned yet:
the published ``wbes_region_id`` is always stored, ``region_code`` stays NULL.
After live verification this flag can be switched on (a reviewed code change).
NLDC (id 6) is not a PI region and always has ``region_code`` NULL.
"""

from __future__ import annotations

EXPECTED_BLOCKS = 96                       # 15-minute blocks per delivery day
NLDC_REGION_ID = 6
WBES_REGION_LABELS: dict[int, str] = {1: "ER", 2: "WR", 3: "NR", 4: "SR", 5: "NER", NLDC_REGION_ID: "NLDC"}
REGION_MAPPING_VERIFIED = False


def is_known_region(region_id: int | None) -> bool:
    return region_id in WBES_REGION_LABELS


def region_code_for(region_id: int | None, *, verified: bool | None = None) -> str | None:
    """PI region code for a WBES region id, only when the mapping is verified; NLDC and unknown ids -> None."""
    if not (REGION_MAPPING_VERIFIED if verified is None else verified):
        return None
    if region_id is None or region_id == NLDC_REGION_ID:
        return None
    return WBES_REGION_LABELS.get(region_id)
