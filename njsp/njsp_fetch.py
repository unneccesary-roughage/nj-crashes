# nj_crashes/njsp_fetch.py

from __future__ import annotations
from datetime import datetime
from pathlib import Path
import re
import requests


BASE_URL = "https://njsp.njoag.gov/wp/wp-content/plugins/fatal-crash-data/xml"


def parse_rundate(xml_content: bytes) -> str | None:
    match = re.search(rb"<RUNDATE>([^<]+)</RUNDATE>", xml_content)
    return match.group(1).decode("utf-8") if match else None


def fetch_year(year: int, out_dir: Path = Path("data/live")) -> tuple[Path, str | None]:
    """
    Download FAUQStats XML for a year.

    Returns:
        (path_to_file, rundate)
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    filename = f"FAUQStats{year}.xml"
    url = f"{BASE_URL}/{filename}"
    out_path = out_dir / filename

    res = requests.get(
        url,
        timeout=20,
        headers={
            "Accept": "text/xml",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        },
    )

    if res.status_code == 404:
        raise FileNotFoundError(f"{filename} not available yet")

    res.raise_for_status()

    content = res.content
    out_path.write_bytes(content)

    rundate = parse_rundate(content)

    return out_path, rundate


def fetch_current_year(out_dir: Path = Path("data/live")):
    return fetch_year(datetime.now().year, out_dir)
