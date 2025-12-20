from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import sys
from dataclasses import dataclass, asdict
from typing import List, Optional

import requests

try:
    from dotenv import load_dotenv  

    load_dotenv()
except Exception:
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as ef:
                for line in ef:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    os.environ.setdefault(k, v)
        except Exception:
            pass

API_BASE = "https://api.sec-api.io"


@dataclass
class Filing:
    accession_number: str
    filing_date: str
    company_name: str
    ticker: Optional[str]
    form_type: str
    filing_url: Optional[str]


def iso_date(dt: datetime.date) -> str:
    return dt.isoformat()


def query_sec_api(query: str, api_key: str) -> dict:
    url = f"{API_BASE}/query"
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    payload = {"query": query}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def build_query(form_type: str, start_date: str, end_date: str) -> str:
    q = f'formType:"{form_type}" AND filingDate:[{start_date} TO {end_date}]'
    return q


def extract_filings(result_json: dict) -> List[Filing]:
    filings: List[Filing] = []
    hits = result_json.get("hits", {}).get("hits", [])
    for h in hits:
        src = h.get("_source", {})
        filings.append(
            Filing(
                accession_number=src.get("accessionNumber") or src.get("accession_number") or "",
                filing_date=src.get("filingDate") or src.get("filedAt") or "",
                company_name=src.get("companyName") or src.get("issuerName") or "",
                ticker=src.get("ticker") or src.get("primaryTicker") or src.get("cik"),
                form_type=src.get("formType") or src.get("form_type") or "",
                filing_url=src.get("htmlUrl") or src.get("filingUrl") or None,
            )
        )
    return filings


def write_outputs(dirpath: str, name: str, filings: List[Filing], dry_run: bool) -> None:
    if not filings:
        print(f"No {name} found for this period.")
        return
    os.makedirs(dirpath, exist_ok=True)
    json_path = os.path.join(dirpath, f"{name}.json")
    csv_path = os.path.join(dirpath, f"{name}.csv")
    if dry_run:
        print(f"Dry-run: would write {len(filings)} records to {json_path} and {csv_path}")
        return
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump([asdict(f) for f in filings], jf, ensure_ascii=False, indent=2)
    with open(csv_path, "w", newline="", encoding="utf-8") as cf:
        writer = csv.writer(cf)
        writer.writerow(["accession_number", "filing_date", "company_name", "ticker", "form_type", "filing_url"])
        for f in filings:
            writer.writerow([f.accession_number, f.filing_date, f.company_name, f.ticker or "", f.form_type, f.filing_url or ""])
    print(f"Wrote {len(filings)} {name} records to {dirpath}")


def period_for_year_month(year: int, month: int) -> tuple[str, str]:
    start = datetime.date(year, month, 1)
    if month == 12:
        end = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        end = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    return iso_date(start), iso_date(end)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Crawl sec-api.io for S-1 and Form 25 filings")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--year-month", help="Year-month in YYYY-MM format to query (e.g. 2025-09)")
    group.add_argument("--last-month", action="store_true", help="Query the previous calendar month")
    p.add_argument("--dry-run", action="store_true", help="Don't write files, just print summary")
    p.add_argument("--output-dir", default="outputs", help="Base output directory")
    args = p.parse_args(argv)

    api_key = os.environ.get("SEC_API_KEY")
    if not api_key:
        print("Environment variable SEC_API_KEY is required. Set it to your sec-api.io API key.")
        return 2

    if args.last_month:
        today = datetime.date.today()
        first_of_this_month = today.replace(day=1)
        last_month_end = first_of_this_month - datetime.timedelta(days=1)
        year = last_month_end.year
        month = last_month_end.month
    elif args.year_month:
        try:
            year, month = map(int, args.year_month.split("-"))
        except Exception:
            print("--year-month must be in YYYY-MM format")
            return 2
    else:
        p.print_help()
        return 2

    start_date, end_date = period_for_year_month(year, month)
    period_dir = os.path.join(args.output_dir, f"{year:04d}-{month:02d}")

    print(f"Querying SEC filings from {start_date} to {end_date} (S-1 for entrants, Form 25 for exits)")

    # Query S-1 filings (IPO/registrations)
    s1_query = build_query("S-1", start_date, end_date)
    try:
        s1_res = query_sec_api(s1_query, api_key)
    except Exception as e:
        print(f"Error querying S-1 filings: {e}")
        return 3
    s1_filings = extract_filings(s1_res)

    # Query Form 25 filings (notices of removal/delistings)
    f25_query = build_query("25", start_date, end_date)
    try:
        f25_res = query_sec_api(f25_query, api_key)
    except Exception as e:
        print(f"Error querying Form 25 filings: {e}")
        return 4
    f25_filings = extract_filings(f25_res)

    write_outputs(period_dir, "entrants", s1_filings, args.dry_run)
    write_outputs(period_dir, "exits", f25_filings, args.dry_run)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
