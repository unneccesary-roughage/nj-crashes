from __future__ import annotations

from pathlib import Path
import hashlib
import sys
import re

from njsp.njsp_fetch import fetch_current_year
from njsp.njsp_parse import parse_fauqstats_crashes
from njsp.sqlite_store import open_db, create_schema, upsert_crashes
from njsp.export_csv import export_all
from njsp.alerts import diff_against_sqlite, format_email, write_alert_files

_RUNDATE_RE = re.compile(rb"<RUNDATE>.*?</RUNDATE>", re.DOTALL)

def sha256_xml_ignoring_rundate(path: Path) -> str:
    data = path.read_bytes()
    data = _RUNDATE_RE.sub(b"<RUNDATE></RUNDATE>", data)
    return hashlib.sha256(data).hexdigest()


def main():
    xml_path, rundate = fetch_current_year(out_dir=Path("data/live"))
    print(f"Fetched XML: {xml_path} (RUNDATE={rundate})")

    hash_path = xml_path.with_suffix(".sha256")
    new_hash = sha256_xml_ignoring_rundate(xml_path)

    if hash_path.exists():
        old_hash = hash_path.read_text().strip()
        if old_hash == new_hash:
            print("XML unchanged — skipping parse, DB update, and exports.")
            sys.exit(0)

    print("XML changed — processing.")

    parsed = parse_fauqstats_crashes(xml_path)
    print(f"Parsed {len(parsed.records)} crash records for {parsed.statsyear}")

    conn = open_db("data/njsp_fatal.sqlite")
    try:
        create_schema(conn)

        # diff BEFORE upsert
        new_recs, upd_recs = diff_against_sqlite(conn, parsed.records)

        n = upsert_crashes(conn, parsed.records)
        print(f"Upserted {n} records into SQLite")

        export_all(conn, "exports")
        print("Exported CSVs to exports/")

        # write alert files only if something meaningful changed
        if new_recs or upd_recs:
            subject, body = format_email(parsed.rundate or "", parsed.statsyear or 0, new_recs, upd_recs)
            write_alert_files(subject, body, "alerts")
            print("Wrote alerts/email_body.txt (will trigger email)")
        else:
            print("No new/updated accidents — no email.")
    finally:
        conn.close()

    # save new hash
    hash_path.write_text(new_hash)
    print("Updated hash file.")


if __name__ == "__main__":
    main()
