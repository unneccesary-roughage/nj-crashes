from __future__ import annotations

import csv
from pathlib import Path
import sqlite3
from typing import Iterable, Tuple, Any


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def export_query_to_csv(
    conn: sqlite3.Connection,
    out_path: Path,
    query: str,
    params: Tuple[Any, ...] = (),
) -> None:
    ensure_dir(out_path.parent)

    cur = conn.cursor()
    cur.execute(query, params)

    colnames = [d[0] for d in cur.description]
    rows = cur.fetchall()

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colnames)
        w.writerows(rows)


def export_all(conn: sqlite3.Connection, exports_dir: str | Path = "exports") -> None:
    exports_dir = Path(exports_dir)

    # 1) Current year crashes (you can change this to a specific year)
    export_query_to_csv(
        conn,
        exports_dir / "njsp_fatal_crashes_all.csv",
        """
        SELECT
          statsyear, accid,
          crash_date, crash_time,
          ccode, cname, mcode, mname,
          highway, location,
          fatalities, fatal_d, fatal_p, fatal_t, fatal_b,
          rundate_raw, fetched_at
        FROM njsp_fatal_crashes
        ORDER BY crash_date DESC, crash_time DESC, accid DESC;
        """,
    )

    # 2) County summary by year
    export_query_to_csv(
        conn,
        exports_dir / "njsp_monthly_totals_by_year.csv",
        """
        SELECT
        statsyear,
        substr(crash_date, 1, 7) AS year_month,
        ccode,
        cname,
        COUNT(*) AS fatal_accidents,
        SUM(fatalities) AS fatalities
        FROM njsp_fatal_crashes
        WHERE crash_date IS NOT NULL
        GROUP BY statsyear, year_month, ccode, cname
        ORDER BY statsyear DESC, year_month DESC, fatalities DESC;
        """,
    )

    # 3) Monthly totals by year
    export_query_to_csv(
        conn,
        exports_dir / "njsp_monthly_totals_by_year.csv",
        """
        SELECT
          statsyear,
          substr(crash_date, 1, 7) AS year_month,
          COUNT(*) AS fatal_accidents,
          SUM(fatalities) AS fatalities
        FROM njsp_fatal_crashes
        WHERE crash_date IS NOT NULL
        GROUP BY statsyear, year_month
        ORDER BY statsyear DESC, year_month DESC;
        """,
    )
