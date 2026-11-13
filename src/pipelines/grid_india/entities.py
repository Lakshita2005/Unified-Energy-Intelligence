"""Names used in the Daily PSP workbook -> regions, states and other drawal entities.

States reuse the reference geography of the CEA pipeline (read-only import:
ISO 3166-2:IN codes, regions as CEA lists them), plus the abbreviations the
PSP uses. Matching is exact after normalisation; an unknown name is reported
(``UNMAPPED_ENTITY``), never guessed.

Approved representation (decision G1):
* STATE: ``area_code = state_code`` (DNHDDPDCL -> DH, published name kept).
* SYSTEM: ``DVC`` (ER), ``JK_LA`` = "J&K(UT) & Ladakh(UT)" (NR).
* BULK_CONSUMER: ``BULK_<region>`` = "Bulk Consumer_<region> ISTS" (the sum of
  the region's Section J entities).
* ENTITY: Section J / ISTS-connected consumers, by normalised name.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.pipelines.cea.geography import STATE_REGION, normalise_name, state_code

REGION_HEADINGS = {"NR": "NR", "WR": "WR", "SR": "SR", "ER": "ER", "NER": "NER"}

# PSP spellings not covered by the shared state names.
STATE_ALIASES = {"UP": "UP", "HP": "HP", "MP": "MP", "DNHDDPDCL": "DH"}

SYSTEMS = {
    "DVC": ("DVC", "ER"),
    "J&K(UT) & LADAKH(UT)": ("JK_LA", "NR"),
    "J&K(UT)&LADAKH(UT)": ("JK_LA", "NR"),
}

# Section J / ISTS-connected consumers seen in the 2023-2026 workbooks, with their region.
ENTITIES = {
    "AMNSIL": ("AMNSIL", "WR"),
    "BALCO": ("BALCO", "WR"),
    "RIL JAMNAGAR": ("RIL_JAMNAGAR", "WR"),
    "WELSPUN": ("WELSPUN", "WR"),
    "HINDUSTAN ZINC LTD. DARIPA": ("HZL_DARIPA", "NR"),
}
_BULK = re.compile(r"^BULK CONSUMER_(NR|WR|SR|ER|NER) ISTS$")
_RAILWAYS = re.compile(r"^RAILWAYS_(NR|WR|SR|ER|NER) ISTS$")


@dataclass(frozen=True)
class Area:
    area_code: str
    area_type: str            # STATE, SYSTEM, BULK_CONSUMER, ENTITY
    state_code: str | None
    region_code: str


def resolve_area(name: str) -> Area | None:
    """Area for a Section C/J row name, or None if the name is not known."""
    key = normalise_name(name)
    if key in STATE_ALIASES:
        code = STATE_ALIASES[key]
        return Area(code, "STATE", code, STATE_REGION[code])
    if key in SYSTEMS:
        code, region = SYSTEMS[key]
        return Area(code, "SYSTEM", None, region)
    if match := _BULK.match(key):
        return Area(f"BULK_{match[1]}", "BULK_CONSUMER", None, match[1])
    if match := _RAILWAYS.match(key):
        return Area(f"RAILWAYS_{match[1]}", "ENTITY", None, match[1])
    if key in ENTITIES:
        code, region = ENTITIES[key]
        return Area(code, "ENTITY", None, region)
    code = state_code(name)
    if code is not None:
        return Area(code, "STATE", code, STATE_REGION[code])
    return None


# States/systems expected in Section C of every workbook (a missing one is flagged).
EXPECTED_SECTION_C = frozenset({
    "PB", "HR", "RJ", "DL", "UP", "UK", "HP", "JK_LA", "CH", "CG", "GJ", "MP", "MH", "GA", "DH", "AP", "TS", "KA",
    "KL", "TN", "PY", "BR", "DVC", "JH", "OD", "WB", "SK", "AR", "AS", "MN", "ML", "MZ", "NL", "TR",
})
