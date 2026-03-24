"""
Anonymize all PII and client-identifiable data across all 5 raw CSVs.

This dataset has been anonymized for public portfolio use:
  - Organization names → business_1, business_2, ...
  - User names         → user_1, user_2, ...
  - Emails / phones    → SHA-256 hashed
  - Passwords          → [REDACTED]
  - Card holder names  → mapped to anonymized user/org names
  - Card nicknames     → org references replaced
  - "fintech_enterprise" references  → "fintech_co"

Original files in data/raw/ are overwritten with anonymized versions.
"""

import csv
import hashlib
import re
import sys
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

# ── Helpers ──────────────────────────────────────────────────────────────────


def sha256(value: str) -> str:
    if not value or not value.strip():
        return ""
    return hashlib.sha256(value.strip().encode("utf-8")).hexdigest()[:16]


def read_csv(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return fieldnames, rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def replace_all_occurrences(text: str, replacements: dict[str, str]) -> str:
    """Apply all replacements (longest match first to avoid partial hits)."""
    for old, new in sorted(replacements.items(), key=lambda x: -len(x[0])):
        text = text.replace(old, new)
    return text


# ── Build mappings ───────────────────────────────────────────────────────────


def build_org_map(rows: list[dict]) -> dict[str, str]:
    """Map org names/variants to business_N."""
    org_map: dict[str, str] = {}
    for i, row in enumerate(rows, start=1):
        label = f"business_{i}"
        name = row.get("name", "").strip()
        bname = row.get("business_name", "").strip()
        if name:
            org_map[name] = label.replace("_", " ").title()  # "Business 1"
            org_map[name.upper()] = label.upper().replace("_", " ")  # "BUSINESS 1"
            org_map[name.lower()] = label  # "business_1"
        if bname:
            org_map[bname] = label
            org_map[bname.lower()] = label
            org_map[bname.title()] = label.replace("_", " ").title()
            org_map[bname.upper()] = label.upper().replace("_", " ")
    return org_map


def build_user_map(rows: list[dict]) -> dict:
    """Map user names to user_N. Returns {full_name: label, first: label_first, ...}."""
    name_map: dict[str, str] = {}
    id_map: dict[str, str] = {}
    for i, row in enumerate(rows, start=1):
        label = f"user_{i}"
        first = row.get("first_name", "").strip()
        last = row.get("last_name", "").strip()
        uid = row.get("id", "").strip()
        full = f"{first} {last}".strip()
        full_upper = full.upper()
        if full:
            name_map[full] = label.replace("_", " ").title()  # "User 1"
            name_map[full_upper] = label.upper().replace("_", " ")  # "USER 1"
        if uid:
            id_map[uid] = label
    return name_map, id_map


# ── Fintech Enterprise replacement ────────────────────────────────────────────────────────

FINTECH_ENTERPRISE_REPLACEMENTS = {
    "FINTECH_ENTERPRISE.IO": "FINTECH_CO.IO",
    "fintech-enterprise.io": "fintech_co.io",
    "Fintech Enterprise": "Fintech Co",
    "FINTECH_ENTERPRISE": "FINTECH_CO",
    "fintech_enterprise": "fintech_co",
}

# ── Anonymize each file ─────────────────────────────────────────────────────


def anonymize_orgs(org_map_raw: dict[int, str]) -> None:
    path = RAW_DIR / "orgs.csv"
    fieldnames, rows = read_csv(path)
    for i, row in enumerate(rows, start=1):
        label = f"business_{i}"
        row["name"] = label.replace("_", " ").title()
        row["business_name"] = label
        row["contact_number"] = ""
    write_csv(path, fieldnames, rows)
    print(f"  orgs.csv: {len(rows)} rows anonymized")


def anonymize_users(user_id_map: dict[str, str]) -> None:
    path = RAW_DIR / "users.csv"
    fieldnames, rows = read_csv(path)
    for i, row in enumerate(rows, start=1):
        label = f"user_{i}"
        row["first_name"] = "User"
        row["last_name"] = str(i)
        row["email"] = sha256(row.get("email", ""))
        row["phone_number"] = sha256(row.get("phone_number", ""))
        row["password"] = "[REDACTED]"
    write_csv(path, fieldnames, rows)
    print(f"  users.csv: {len(rows)} rows anonymized")


def anonymize_cards(org_map: dict[str, str], user_name_map: dict[str, str]) -> None:
    path = RAW_DIR / "cards.csv"
    fieldnames, rows = read_csv(path)
    # Combined replacement map: org names + user names + fintech_enterprise
    replacements = {**FINTECH_ENTERPRISE_REPLACEMENTS, **org_map, **user_name_map}
    for row in rows:
        name_on_card = row.get("name_on_card", "")
        if name_on_card:
            row["name_on_card"] = replace_all_occurrences(name_on_card, replacements)
        nickname = row.get("card_nickname", "")
        if nickname:
            row["card_nickname"] = replace_all_occurrences(nickname, replacements)
    write_csv(path, fieldnames, rows)
    print(f"  cards.csv: {len(rows)} rows anonymized")


def anonymize_transactions(org_map: dict[str, str]) -> None:
    path = RAW_DIR / "transactions.csv"
    fieldnames, rows = read_csv(path)
    replacements = {**FINTECH_ENTERPRISE_REPLACEMENTS, **org_map}
    for row in rows:
        merchant = row.get("merchant", "")
        if merchant:
            row["merchant"] = replace_all_occurrences(merchant, replacements)
    write_csv(path, fieldnames, rows)
    print(f"  transactions.csv: {len(rows)} rows anonymized")


def anonymize_clearing(org_map: dict[str, str]) -> None:
    path = RAW_DIR / "clearing.csv"
    fieldnames, rows = read_csv(path)
    replacements = {**FINTECH_ENTERPRISE_REPLACEMENTS, **org_map}
    for row in rows:
        # Tenant Name
        tenant = row.get("Tenant Name", "")
        if tenant:
            row["Tenant Name"] = replace_all_occurrences(tenant, replacements)
        # Merchant Name
        merchant = row.get("Merchant Name", "")
        if merchant:
            row["Merchant Name"] = replace_all_occurrences(merchant, replacements)
        # Card Holder (numeric IDs in this dataset, but scrub just in case)
        cardholder = row.get("Card Holder", "")
        if cardholder:
            row["Card Holder"] = replace_all_occurrences(cardholder, replacements)
    write_csv(path, fieldnames, rows)
    print(f"  clearing.csv: {len(rows)} rows anonymized")


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    print("Anonymizing all raw CSVs in data/raw/...")
    print("NOTE: This overwrites the original files.\n")

    # Step 1: Build mappings from current (un-anonymized) data
    _, org_rows = read_csv(RAW_DIR / "orgs.csv")
    _, user_rows = read_csv(RAW_DIR / "users.csv")

    org_map = build_org_map(org_rows)
    user_name_map, user_id_map = build_user_map(user_rows)

    # Step 2: Anonymize each file
    anonymize_orgs(org_map)
    anonymize_users(user_id_map)
    anonymize_cards(org_map, user_name_map)
    anonymize_transactions(org_map)
    anonymize_clearing(org_map)

    print("\nDone. All organization names → business_N, user names → user_N,")
    print("emails/phones → SHA-256 hashed, passwords → [REDACTED],")
    print("'fintech_enterprise' references → 'fintech_co'.")
    print("\nThese files are now safe for public GitHub / Upwork showcase.")


if __name__ == "__main__":
    main()
