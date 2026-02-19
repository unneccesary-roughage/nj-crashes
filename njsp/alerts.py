from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional


def _chunks(lst, n=900):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def diff_against_sqlite(conn: sqlite3.Connection, records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (new_records, updated_records) based on (STATSYEAR, ACCID) and record_hash.
    """
    keys = []
    for r in records:
        try:
            statsyear = int(r["STATSYEAR"])
            accid = int(r["ACCID"])
        except Exception:
            continue
        keys.append((statsyear, accid))

    if not keys:
        return [], []

    # For this feed, all records are same year; still handle generically.
    existing: Dict[Tuple[int, int], Optional[str]] = {}

    # SQLite has parameter limits; chunk IN lists.
    for chunk in _chunks(keys, 800):
        # Build WHERE (statsyear=? AND accid=?) OR ... safely
        where = " OR ".join(["(statsyear=? AND accid=?)"] * len(chunk))
        params = [p for pair in chunk for p in pair]

        rows = conn.execute(
            f"SELECT statsyear, accid, record_hash FROM njsp_fatal_crashes WHERE {where}",
            params,
        ).fetchall()

        for y, a, h in rows:
            existing[(y, a)] = h

    new_recs = []
    upd_recs = []

    for r in records:
        try:
            y = int(r["STATSYEAR"])
            a = int(r["ACCID"])
        except Exception:
            continue
        key = (y, a)
        new_hash = r.get("record_hash")

        if key not in existing:
            new_recs.append(r)
        else:
            old_hash = existing.get(key)
            if old_hash and new_hash and old_hash != new_hash:
                upd_recs.append(r)

    return new_recs, upd_recs


def format_email(rundate: str, year: int, new_recs: List[Dict[str, Any]], upd_recs: List[Dict[str, Any]]) -> Tuple[str, str]:
    """
    Returns (subject, body)
    """
    subject = f"NJSP fatal crashes update ({year}) — {len(new_recs)} new, {len(upd_recs)} updated"

    lines = []
    lines.append(f"RUNDATE: {rundate}")
    lines.append("")
    lines.append(f"New crashes: {len(new_recs)}")
    lines.append(f"Updated crashes: {len(upd_recs)}")
    lines.append("")

    def format_time_ampm(s: str | None) -> str:
        if not s:
            return "Unknown"

        s = s.strip().zfill(4)

        try:
            hh = int(s[:2])
            mm = int(s[2:])
        except Exception:
            return s  # fallback

        suffix = "AM"
        if hh == 0:
            hh = 12
        elif hh == 12:
            suffix = "PM"
        elif hh > 12:
            hh -= 12
            suffix = "PM"

        return f"{hh}:{mm:02d} {suffix}"


    def one_line(r: Dict[str, Any]) -> str:
        killed = []

        def add(role: str, key: str):
            try:
                n = int(r.get(key) or 0)
            except Exception:
                n = 0
            if n > 0:
                killed.append(f"{role} ({n})")

        add("Driver", "FATAL_D")
        add("Passenger", "FATAL_P")
        add("Pedestrian", "FATAL_T")
        add("Pedalcyclist", "FATAL_B")

        if not killed:
            killed_txt = "Unknown"
        else:
            killed_txt = ", ".join(killed)

        return (
            f"ACCID {r.get('ACCID')} | {r.get('DATE')} {format_time_ampm(r.get('TIME'))} | "
            f"{r.get('MNAME')}, {r.get('CNAME')} | "
            f"{r.get('HIGHWAY')} | {r.get('LOCATION')} | "
            f"Killed: {killed_txt}"
    )


    if new_recs:
        lines.append("NEW:")
        for r in new_recs[:25]:
            lines.append("  - " + one_line(r))
        if len(new_recs) > 25:
            lines.append(f"  ...and {len(new_recs) - 25} more")
        lines.append("")

    if upd_recs:
        lines.append("UPDATED:")
        for r in upd_recs[:25]:
            lines.append("  - " + one_line(r))
        if len(upd_recs) > 25:
            lines.append(f"  ...and {len(upd_recs) - 25} more")
        lines.append("")

    return subject, "\n".join(lines)


def write_alert_files(subject: str, body: str, alerts_dir: str | Path = "alerts") -> None:
    alerts_dir = Path(alerts_dir)
    alerts_dir.mkdir(parents=True, exist_ok=True)
    (alerts_dir / "email_subject.txt").write_text(subject, encoding="utf-8")
    (alerts_dir / "email_body.txt").write_text(body, encoding="utf-8")
    # flag file for Actions
    (alerts_dir / "send_email.flag").write_text("1", encoding="utf-8")
