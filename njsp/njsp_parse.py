# njsp/njsp_parse.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import hashlib
import re

from lxml import etree

_RUNDATE_RE = re.compile(rb"<RUNDATE>([^<]+)</RUNDATE>", re.IGNORECASE)

@dataclass(frozen=True)
class ParseResult:
    rundate: Optional[str]
    statsyear: Optional[int]
    records: List[Dict[str, Optional[str]]]

def parse_rundate(xml_bytes: bytes) -> Optional[str]:
    m = _RUNDATE_RE.search(xml_bytes)
    return m.group(1).decode("utf-8").strip() if m else None

def _t(root: etree._Element, tag: str) -> Optional[str]:
    el = root.find(tag)
    return (el.text or "").strip() if el is not None and el.text else None

def parse_fauqstats_crashes(xml_path: str | Path, add_hash: bool = True) -> ParseResult:
    """
    Parse FAUQStats XML into one record per <ACCIDENT>.
    Pulls COUNTY/MUNICIPALITY attributes from ancestors and merges them into each record.
    """
    xml_path = Path(xml_path)
    xml_bytes = xml_path.read_bytes()
    rundate = parse_rundate(xml_bytes)

    root = etree.fromstring(xml_bytes)

    statsyear_txt = _t(root, "STATSYEAR")
    statsyear = int(statsyear_txt) if statsyear_txt and statsyear_txt.isdigit() else None

    records: List[Dict[str, Optional[str]]] = []

    # Find all ACCIDENT elements regardless of namespace (this file seems no-namespace)
    for accident in root.xpath(".//ACCIDENT"):
        rec: Dict[str, Optional[str]] = {}

        # ACCIDENT attributes like ACCID, DATE, TIME
        for k, v in (accident.attrib or {}).items():
            rec[k] = (v or "").strip() or None

        # Parent MUNICIPALITY attributes
        muni = accident.getparent()
        if muni is not None and isinstance(muni.tag, str) and muni.tag.upper().endswith("MUNICIPALITY"):
            for k, v in (muni.attrib or {}).items():
                rec[k] = (v or "").strip() or None

        # Parent COUNTY attributes (COUNTY is parent of MUNICIPALITY)
        county = muni.getparent() if muni is not None else None
        if county is not None and isinstance(county.tag, str) and county.tag.upper().endswith("COUNTY"):
            for k, v in (county.attrib or {}).items():
                rec[k] = (v or "").strip() or None

        # Child elements inside ACCIDENT (HIGHWAY, LOCATION, FATALITIES...)
        for child in accident:
            if not isinstance(child.tag, str):
                continue
            key = child.tag
            val = (child.text or "").strip() or None
            rec[key] = val

        # Metadata
        rec["RUNDATE"] = rundate
        rec["STATSYEAR"] = str(statsyear) if statsyear is not None else None

        # Stable record hash (useful for deduping if ACCID isn’t globally unique)
        if add_hash:
            hash_exclude = {"RUNDATE", "record_hash"}
            items = sorted(
                (k, "" if v is None else v)
                for k, v in rec.items()
                if k not in hash_exclude
            )
            rec["record_hash"] = hashlib.sha256(repr(items).encode("utf-8")).hexdigest()

        records.append(rec)

    return ParseResult(rundate=rundate, statsyear=statsyear, records=records)
