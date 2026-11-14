"""Grid-India reference data: countries and the international line -> country table.

Approved with the IR-Line audit (2026-11-14):

* countries use ISO 3166-1 alpha-3 codes (``pi.country.country_code`` is
  ``varchar(3)``) and are seeded idempotently by
  ``python -m scripts.ingest_grid_india reference`` (never implicitly by a run);
* the international part of the ``IR-Line`` sheet prints the country name on
  only one row of each group, so the country of a line comes from an explicit
  **exact line-name** table (whitespace-normalised, case-insensitive). No
  keyword matching and no positional inference: an unknown line name is
  rejected as ``UNMAPPED_COUNTRY``. The table holds every name published in
  the 2023-2026 workbooks inspected (16); its grouping reconciles with the
  Section D transnational totals.

Extended with the CrossBorder audit (2026-11-14, decision C2): the
'CrossBorder' sheet has a row for Myanmar (always zero in 2023-07..2026-11),
so MMR is added. CrossBorder country rows are mapped by the exact published
label (``CROSSBORDER_COUNTRIES``); an unknown label is rejected.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.pipelines.cea.cells import norm

COUNTRIES: dict[str, str] = {"BTN": "Bhutan", "NPL": "Nepal", "BGD": "Bangladesh", "MMR": "Myanmar"}
# Published country label (State cell) -> code
COUNTRY_LABELS: dict[str, str] = {"BHUTAN": "BTN", "NEPAL": "NPL", "BANGLADESH": "BGD"}
# CrossBorder sheet country label (normalised) -> code. Exact labels only.
CROSSBORDER_COUNTRIES: dict[str, str] = {"BHUTAN": "BTN", "NEPAL": "NPL", "BANGLADESH": "BGD", "MYANMAR": "MMR"}

# Exact published international line names (as in the workbooks) -> country code.
INTERNATIONAL_LINES: dict[str, str] = {norm(name): code for name, code in (
    # Bhutan
    ("220kV CHUKHA-BIRPARA 1&2 (& 220kV MALBASE - BIRPARA) i.e. BIRPARA RECEIPT (from CHUKHA HEP 4*84MW)", "BTN"),
    ("132kV GELEPHU-SALAKATI", "BTN"),
    ("132kV MOTANGA-RANGIA", "BTN"),
    ("400kV JIGMELING-ALIPURDUAR 1,2, 400kV PUNATSANGCHHU - ALIPURDUAR 1,2 i.e. ALIPURDUAR RECEIPT (from MANGDECHU "
     "HEP 4*180MW & PUNATSANGCHHU HEP 6*170MW)", "BTN"),
    ("400kV MANGDECHHU-ALIPURDUAR 1,2&3 i.e. ALIPURDUAR RECEIPT (from MANGDECHU HEP 4*180MW)", "BTN"),
    ("400kV TALA-BINAGURI 1,2 ,400kV MALBASE - BINAGURI & 400kV NORBUGANG - BINAGURI i.e. BINAGURI RECEIPT (from "
     "TALA HEP 6*170MW)", "BTN"),
    ("400kV TALA-BINAGURI 1,2,4 (& 400kV MALBASE - BINAGURI) i.e. BINAGURI RECEIPT (from TALA HEP (6*170MW)", "BTN"),
    ("400kV TALA-BINAGURI 1,2,4 (& 400kV MALBASE - BINAGURI) i.e. BINAGURI RECEIPT (from TALA HEP 6*170MW)", "BTN"),
    # Nepal
    ("132kV MAHENDRANAGAR-TANAKPUR(NHPC)", "NPL"),
    ("NEPAL IMPORT (FROM UP)", "NPL"),
    ("NEPAL IMPORT (FROM BIHAR)", "NPL"),
    ("400kV DHALKEBAR-MUZAFFARPUR 1&2", "NPL"),
    # Bangladesh
    ("BHERAMARA B/B HVDC (B'DESH)", "BGD"),
    ("BHERAMARA B/B HVDC (BANGLADESH)", "BGD"),
    ("400kV GODDA_TPS-RAHANPUR (B'DESH) D/C", "BGD"),
    ("132kV COMILLA-SURAJMANI NAGAR 1&2", "BGD"),
)}


def line_country(line_name: str) -> str | None:
    """Country code of a published international line name, or None if the exact name is not in the table."""
    return INTERNATIONAL_LINES.get(norm(line_name))


@dataclass
class CountrySeedResult:
    inserted: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    differing: list[str] = field(default_factory=list)


def seed_countries(connection: Connection, schema: str) -> CountrySeedResult:
    """Insert missing countries (``ON CONFLICT DO NOTHING``); existing rows are never changed."""
    result = CountrySeedResult()
    for code, name in COUNTRIES.items():
        inserted = connection.execute(text(
            f"INSERT INTO {schema}.country (country_code, country_name) VALUES (:c, :n) "
            "ON CONFLICT (country_code) DO NOTHING RETURNING country_code"), {"c": code, "n": name}).scalar_one_or_none()
        if inserted:
            result.inserted.append(code)
            continue
        stored = connection.execute(text(f"SELECT country_name FROM {schema}.country WHERE country_code = :c"),
                                    {"c": code}).scalar_one()
        (result.unchanged if stored == name else result.differing).append(code)
    return result


def missing_countries(connection: Connection, schema: str) -> list[str]:
    present = {r[0] for r in connection.execute(text(f"SELECT country_code FROM {schema}.country"))}
    return [code for code in COUNTRIES if code not in present]
