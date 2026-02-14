from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Any, Iterable, Optional
from datetime import datetime, date, time
import re


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_date_mmddyyyy(s: Optional[str]) -> Optional[str]:
    """Return ISO date string YYYY-MM-DD for SQLite, or None."""
    if not s:
        return None
    dt = datetime.strptime(s, "%m/%d/%Y").date()
    return dt.isoformat()


def parse_time_hhmm(s: Optional[str]) -> Optional[str]:
    """Return HH:MM:SS string for SQLite, or None."""
    if not s:
        return None
    s = s.strip().zfill(4)
    hh = int(s[:2])
    mm = int(s[2:])
    return f"{hh:02d}:{mm:02d}:00"


def to_int(s: Optional[str]) -> Optional[int]:
    if s is None or s == "":
        return None
    return int(s)


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS njsp_fatal_crashes (
          statsyear     INTEGER NOT NULL,
          accid         INTEGER NOT NULL,

          crash_date    TEXT,   -- YYYY-MM-DD
          crash_time    TEXT,   -- HH:MM:SS

          ccode         TEXT,
          cname         TEXT,
          mcode         TEXT,
          mname         TEXT,

          highway       TEXT,
          location      TEXT,

          fatalities    INTEGER,
          fatal_d       INTEGER,
          fatal_p       INTEGER,
          fatal_t       INTEGER,
          fatal_b       INTEGER,

          rundate_raw   TEXT,
          record_hash   TEXT,
          fetched_at    TEXT NOT NULL DEFAULT (datetime('now')),

          PRIMARY KEY (statsyear, accid)
        );
        """
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_njsp_fatal_crashes_date ON njsp_fatal_crashes (crash_date);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_njsp_fatal_crashes_county ON njsp_fatal_crashes (ccode);")
    conn.commit()


def open_db(db_path: str | Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    ensure_parent_dir(db_path)
    conn = sqlite3.connect(str(db_path))
    # safer defaults
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def upsert_crashes(conn: sqlite3.Connection, records: Iterable[Dict[str, Any]]) -> int:
    """
    Upsert per-crash records into SQLite.
    Returns number of records processed.
    """
    sql = """
    INSERT INTO njsp_fatal_crashes (
      statsyear, accid,
      crash_date, crash_time,
      ccode, cname, mcode, mname,
      highway, location,
      fatalities, fatal_d, fatal_p, fatal_t, fatal_b,
      rundate_raw, record_hash, fetched_at
    ) VALUES (
      ?, ?,
      ?, ?,
      ?, ?, ?, ?,
      ?, ?,
      ?, ?, ?, ?, ?,
      ?, ?, datetime('now')
    )
    ON CONFLICT(statsyear, accid) DO UPDATE SET
      crash_date   = excluded.crash_date,
      crash_time   = excluded.crash_time,
      ccode        = excluded.ccode,
      cname        = excluded.cname,
      mcode        = excluded.mcode,
      mname        = excluded.mname,
      highway      = excluded.highway,
      location     = excluded.location,
      fatalities   = excluded.fatalities,
      fatal_d      = excluded.fatal_d,
      fatal_p      = excluded.fatal_p,
      fatal_t      = excluded.fatal_t,
      fatal_b      = excluded.fatal_b,
      rundate_raw  = excluded.rundate_raw,
      record_hash  = excluded.record_hash,
      fetched_at   = datetime('now')
    ;
    """

    cur = conn.cursor()
    n = 0
    for r in records:
        statsyear = to_int(r.get("STATSYEAR"))
        accid = to_int(r.get("ACCID"))
        if statsyear is None or accid is None:
            # skip malformed rows
            continue

        cur.execute(
            sql,
            (
                statsyear,
                accid,
                parse_date_mmddyyyy(r.get("DATE")),
                parse_time_hhmm(r.get("TIME")),
                r.get("CCODE"),
                r.get("CNAME"),
                r.get("MCODE"),
                r.get("MNAME"),
                r.get("HIGHWAY"),
                r.get("LOCATION"),
                to_int(r.get("FATALITIES")),
                to_int(r.get("FATAL_D")),
                to_int(r.get("FATAL_P")),
                to_int(r.get("FATAL_T")),
                to_int(r.get("FATAL_B")),
                r.get("RUNDATE"),
                r.get("record_hash"),
            ),
        )
        n += 1

    conn.commit()
    return n
