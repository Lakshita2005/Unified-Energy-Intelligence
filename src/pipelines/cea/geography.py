"""Regions, states/UTs and the names CEA reports use for them.

State codes are the ISO 3166-2:IN subdivision codes without the ``IN-``
prefix (current codes: CG, OD, TS, UK). Regions are the five grid regions.
The region of a state is the one CEA's Power Supply Position report lists it
under (Lakshadweep: Southern, Andaman & Nicobar: Eastern).

Name matching is exact after normalisation (:func:`normalise_name`); there is
no fuzzy matching. A name that is not listed here is reported, never guessed.
"""

from __future__ import annotations

import re

REGIONS: dict[str, str] = {
    "NR": "Northern Region",
    "WR": "Western Region",
    "SR": "Southern Region",
    "ER": "Eastern Region",
    "NER": "North-Eastern Region",
}

# (code, official name, region, extra names used in CEA reports)
STATES: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("AN", "Andaman and Nicobar Islands", "ER", ("ANDAMAN & NICOBAR ISLANDS", "ANDAMAN-NICOBAR", "ANDAMAN & NICOBAR")),
    ("AP", "Andhra Pradesh", "SR", ()),
    ("AR", "Arunachal Pradesh", "NER", ()),
    ("AS", "Assam", "NER", ()),
    ("BR", "Bihar", "ER", ()),
    ("CH", "Chandigarh", "NR", ()),
    ("CG", "Chhattisgarh", "WR", ("CHHATISGARH",)),
    ("DH", "Dadra and Nagar Haveli and Daman and Diu", "WR", ("DADRA & NAGAR HAVELI AND DAMAN & DIU",)),
    ("DL", "Delhi", "NR", ()),
    ("GA", "Goa", "WR", ()),
    ("GJ", "Gujarat", "WR", ()),
    ("HR", "Haryana", "NR", ()),
    ("HP", "Himachal Pradesh", "NR", ()),
    ("JK", "Jammu and Kashmir", "NR", ("JAMMU & KASHMIR",)),
    ("JH", "Jharkhand", "ER", ()),
    ("KA", "Karnataka", "SR", ()),
    ("KL", "Kerala", "SR", ("KERALAM",)),
    ("LA", "Ladakh", "NR", ()),
    ("LD", "Lakshadweep", "SR", ()),
    ("MP", "Madhya Pradesh", "WR", ()),
    ("MH", "Maharashtra", "WR", ()),
    ("MN", "Manipur", "NER", ()),
    ("ML", "Meghalaya", "NER", ()),
    ("MZ", "Mizoram", "NER", ()),
    ("NL", "Nagaland", "NER", ()),
    ("OD", "Odisha", "ER", ("ORISSA",)),
    ("PY", "Puducherry", "SR", ("PONDICHERRY",)),
    ("PB", "Punjab", "NR", ()),
    ("RJ", "Rajasthan", "NR", ()),
    ("SK", "Sikkim", "ER", ()),
    ("TN", "Tamil Nadu", "SR", ()),
    ("TS", "Telangana", "SR", ()),
    ("TR", "Tripura", "NER", ()),
    ("UP", "Uttar Pradesh", "NR", ()),
    ("UK", "Uttarakhand", "NR", ()),
    ("WB", "West Bengal", "ER", ()),
)

# Region headings in DGR sub-report 2.
DGR_REGION_HEADINGS: dict[str, str] = {
    "NORTHERN": "NR", "WESTERN": "WR", "SOUTHERN": "SR", "EASTERN": "ER", "NORTH EASTERN": "NER",
}

# Region total rows in the monthly Power Supply Position report.
PSP_REGION_NAMES: dict[str, str] = {
    "NORTHERN REGION": "NR", "WESTERN REGION": "WR", "SOUTHERN REGION": "SR", "EASTERN REGION": "ER",
    "NORTH-EASTERN REGION": "NER", "NORTH EASTERN REGION": "NER",
}


def normalise_name(name: str) -> str:
    """Upper case, single spaces, no spaces around hyphens: "Andaman- Nicobar" -> "ANDAMAN-NICOBAR"."""
    text = re.sub(r"\s+", " ", name).strip().upper()
    return re.sub(r"\s*-\s*", "-", text)


STATE_BY_NAME: dict[str, str] = {}
STATE_REGION: dict[str, str] = {}
for _code, _name, _region, _aliases in STATES:
    STATE_REGION[_code] = _region
    for _alias in (_name, _name.replace(" and ", " & "), *_aliases):
        STATE_BY_NAME[normalise_name(_alias)] = _code


def state_code(name: str) -> str | None:
    return STATE_BY_NAME.get(normalise_name(name))
