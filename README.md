# NJSP Fatal Crash Data Pipeline

Automated daily pipeline that:

1.  Downloads the New Jersey State Police **FAUQStats** XML feed\
2.  Parses individual fatal crash records\
3.  Maintains a clean, deduplicated SQLite database\
4.  Exports Excel-ready CSV files\
5.  Sends email alerts when new or updated crashes appear

The system is designed to run in **GitHub Actions** with no servers
required.

------------------------------------------------------------------------

## Data Flow

NJSP Website\
→ `data/live/FAUQStatsYYYY.xml`\
→ Parse `<ACCIDENT>` records\
→ Upsert into `data/njsp_fatal.sqlite`\
→ Export CSV → `exports/`\
→ Optional email alert

The XML is considered temporary input.\
SQLite + CSV are the durable products.

------------------------------------------------------------------------

## Repository Structure

    njsp/
      njsp_fetch.py        # downloads XML from NJSP
      njsp_parse.py        # converts XML → accident records
      sqlite_store.py      # database schema + upsert logic
      export_csv.py        # produces Excel-friendly outputs
      alerts.py            # detects new/updated crashes + email content
      run_pipeline.py      # orchestration entry point

    data/
      njsp_fatal.sqlite    # main database
      live/                # downloaded XML + hash

    exports/
      *.csv                # reporting outputs for Excel

    .github/workflows/
      daily.yml            # automation

------------------------------------------------------------------------

## What counts as a "change"

The pipeline hashes the downloaded XML.

If the hash is unchanged:

    skip parse  
    skip database  
    skip exports  
    skip email  

This prevents unnecessary commits and noise.

------------------------------------------------------------------------

## Database

SQLite file:

    data/njsp_fatal.sqlite

Primary key:

    (statsyear, accid)

If NJSP corrects a crash later, it is automatically updated.

------------------------------------------------------------------------

## CSV Outputs

Located in:

    exports/

Designed for easy use in Excel / Power Query.

Examples: - full accident list\
- county summaries\
- monthly summaries

Files regenerate only when NJSP data changes.

------------------------------------------------------------------------

## Email Alerts

When new or updated crashes are detected, the pipeline creates:

    alerts/email_subject.txt
    alerts/email_body.txt
    alerts/send_email.flag

GitHub Actions sends the email **only if the flag exists**.

Emails include: - crash id\
- date\
- time (converted to AM/PM)\
- county / municipality\
- highway / location\
- who was killed, with counts

Example:

    ACCID 14411 | 02/11/2026 8:32 PM | Essex / Newark City | 601 | County 601 | Killed: Pedestrian (1)

------------------------------------------------------------------------

## Running Locally

From repo root:

    python -m njsp.run_pipeline

------------------------------------------------------------------------

## Force email generation (testing)

    FORCE_EMAIL=1 python -m njsp.run_pipeline

------------------------------------------------------------------------

## GitHub Actions

The workflow can run:

-   on a daily schedule\
-   manually via button\
-   optionally on push (recommended during development)

It will:

1.  Fetch XML\
2.  Detect change\
3.  Update SQLite\
4.  Export CSV\
5.  Send email (if needed)\
6.  Commit outputs

------------------------------------------------------------------------

## Why SQLite

We use SQLite because it:

-   requires no server\
-   works perfectly in automation\
-   supports keys and upserts\
-   is easily migrated to PostgreSQL later

------------------------------------------------------------------------

## Future Upgrades (easy from here)

-   publish dashboards\
-   add geocoding\
-   map links in email\
-   pedestrian emphasis\
-   weekly/monthly trend reports\
-   migration to Postgres/PostGIS

------------------------------------------------------------------------

## Ownership

This pipeline is intentionally simple, transparent, and maintainable.\
All logic is visible in Python --- no hidden services.
