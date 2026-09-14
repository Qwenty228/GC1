"""Fetch SEC EDGAR submissions for a company and save as JSON, plus a generated data dictionary.

Data dictionary / API docs (official, not fetched):
  - EDGAR APIs overview:     https://www.sec.gov/search-filings/edgar-application-programming-interfaces
  - Fair access / User-Agent: https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data
  - Filing form types:       https://www.sec.gov/forms
  - Bulk submissions ZIP:    https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip
"""

import json
from pathlib import Path
from urllib.request import Request, urlopen

CIK = "0001045810"  # NVIDIA
URL = f"https://data.sec.gov/submissions/CIK{CIK}.json"
USER_AGENT = "StudentResearchProject nathankclaude@gmail.com"
OUT_DIR = Path(__file__).parent / "data"


def fetch(url: str) -> dict:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"})
    with urlopen(req, timeout=30) as resp:
        return json.load(resp)


def build_data_dict(obj, prefix="") -> list[dict]:
    """Walk the JSON and describe every field: path, type, and an example value."""
    rows = []
    if isinstance(obj, dict):
        for key, val in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            if isinstance(val, dict):
                rows.append({"field": path, "type": "object", "example": None})
                rows.extend(build_data_dict(val, path))
            elif isinstance(val, list):
                # filings.recent.* are parallel arrays: one element per filing
                elem = val[0] if val else None
                rows.append({
                    "field": path,
                    "type": f"array<{type(elem).__name__ if elem is not None else 'unknown'}>",
                    "length": len(val),
                    "example": elem if not isinstance(elem, (dict, list)) else None,
                })
                if isinstance(elem, dict):
                    rows.extend(build_data_dict(elem, f"{path}[]"))
            else:
                rows.append({"field": path, "type": type(val).__name__, "example": val})
    return rows


def main():
    OUT_DIR.mkdir(exist_ok=True)

    data = fetch(URL)
    out_file = OUT_DIR / f"submissions_CIK{CIK}.json"
    out_file.write_text(json.dumps(data, indent=2))
    print(f"Saved {out_file}")

    dd_file = OUT_DIR / f"submissions_CIK{CIK}_datadict.json"
    dd_file.write_text(json.dumps(build_data_dict(data), indent=2, default=str))
    print(f"Saved {dd_file}")


if __name__ == "__main__":
    main()
