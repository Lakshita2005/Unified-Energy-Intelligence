"""IMD reference values.

``AWS_STATE_IDS`` reproduces the 36 state ids documented for the ``sid`` parameter of
``/api/v1/aws_data`` (API reference). ``IMD_STATE_TO_PI`` maps those exact published names to
``pi.state.state_code``. Only exact names map (after trimming and upper-casing); anything else is
left unmapped and reported, never guessed.

Note: IMD lists DAMAN_AND_DIU separately; since 2020 it is part of the union territory
"Dadra and Nagar Haveli and Daman and Diu" (``pi.state`` code DH), so it maps to DH.
Whether the AWS ``STATE`` field uses exactly these spellings is UNVERIFIED (the documented
sample shows "DELHI").
"""

from __future__ import annotations

AWS_NETWORK = "AWS_ARG"

AWS_STATE_IDS: dict[int, str] = {
    1: "TELANGANA", 2: "ANDHRA_PRADESH", 3: "HIMACHAL_PRADESH", 4: "KERALA", 5: "UTTAR_PRADESH",
    6: "MEGHALAYA", 7: "DELHI", 8: "RAJASTHAN", 9: "GUJARAT", 10: "ODISHA", 11: "BIHAR", 12: "CHHATTISGARH",
    13: "KARNATAKA", 14: "MIZORAM", 15: "JHARKHAND", 16: "TRIPURA", 17: "CHANDIGARH", 18: "JAMMU_AND_KASHMIR",
    19: "GOA", 20: "SIKKIM", 21: "MAHARASHTRA", 22: "HARYANA", 23: "LADAKH", 24: "ASSAM", 25: "TAMIL_NADU",
    26: "WEST_BENGAL", 27: "MADHYA_PRADESH", 28: "ARUNACHAL_PRADESH", 29: "LAKSHADWEEP", 30: "MANIPUR",
    31: "UTTARAKHAND", 32: "NAGALAND", 33: "PUDUCHERRY", 34: "PUNJAB", 35: "ANDAMAN_AND_NICOBAR",
    36: "DAMAN_AND_DIU",
}

IMD_STATE_TO_PI: dict[str, str] = {
    "TELANGANA": "TS", "ANDHRA_PRADESH": "AP", "HIMACHAL_PRADESH": "HP", "KERALA": "KL", "UTTAR_PRADESH": "UP",
    "MEGHALAYA": "ML", "DELHI": "DL", "RAJASTHAN": "RJ", "GUJARAT": "GJ", "ODISHA": "OD", "BIHAR": "BR",
    "CHHATTISGARH": "CG", "KARNATAKA": "KA", "MIZORAM": "MZ", "JHARKHAND": "JH", "TRIPURA": "TR",
    "CHANDIGARH": "CH", "JAMMU_AND_KASHMIR": "JK", "GOA": "GA", "SIKKIM": "SK", "MAHARASHTRA": "MH",
    "HARYANA": "HR", "LADAKH": "LA", "ASSAM": "AS", "TAMIL_NADU": "TN", "WEST_BENGAL": "WB",
    "MADHYA_PRADESH": "MP", "ARUNACHAL_PRADESH": "AR", "LAKSHADWEEP": "LD", "MANIPUR": "MN",
    "UTTARAKHAND": "UK", "NAGALAND": "NL", "PUDUCHERRY": "PY", "PUNJAB": "PB", "ANDAMAN_AND_NICOBAR": "AN",
    "DAMAN_AND_DIU": "DH",
}
assert set(IMD_STATE_TO_PI) == set(AWS_STATE_IDS.values())


def state_code_for(state_name: str | None) -> str | None:
    """PI state code for an exact published IMD state name, or None."""
    if state_name is None:
        return None
    return IMD_STATE_TO_PI.get(state_name.strip().upper())
