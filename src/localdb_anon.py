#!/usr/bin/env python3
r"""
localdb_anon.py  (v1.0.2)
Deterministic, in-place anonymization for a TDC Sessions/local.db

- Edits the DB IN PLACE.
- Optionally makes a temp copy (outside case dir) for proof.
- Can be run standalone or imported.

Usage (standalone):
  python localdb_anon.py --case-dir "D:\Data_Clean\017_01-474" --norm-id 017_01-474
  # or point directly at the DB
  python localdb_anon.py --db "D:\Data_Clean\017_01-474\...\local.db"
"""

from __future__ import annotations
import argparse, sqlite3, tempfile, shutil, json, re, hashlib, datetime, logging, sys
from pathlib import Path

_SALT = "pedaprocanon-2025-09-30"

_PATTERNS = [
    (r'^(mrn|medical[_ ]?record[_ ]?number|patient[_ ]?mrn)$', "id_hash_numeric"),
    (r'^(patient[_ ]?id|patientid|pid)$', "id_hash_numeric"),
    (r'^(case[_ ]?id|caseid)$', "id_hash_alnum"),
    (r'^(session[_ ]?id|sessionid)$', "id_hash_alnum"),
    (r'^(study[_ ]?id|studyid)$', "id_hash_alnum"),
    (r'^id$', "id_passthrough"),
    (r'^(first[_ ]?name|given[_ ]?name|fname)$', "first_name"),
    (r'^(last[_ ]?name|surname|lname|family[_ ]?name)$', "last_name"),
    (r'^(name|full[_ ]?name|patient[_ ]?name)$', "full_name"),
    (r'^(email|e[_ ]?mail)$', "email"),
    (r'^(phone|phone[_ ]?number|mobile|cell)$', "phone"),
    (r'^(address|street|street[_ ]?1|street[_ ]?2|addr1|addr2)$', "address"),
    (r'^(city|town)$', "city"),
    (r'^(state|province|region)$', "state"),
    (r'^(zip|zipcode|postal[_ ]?code)$', "zip"),
    (r'^(dob|date[_ ]?of[_ ]?birth|birth[_ ]?date)$', "dob"),
    (r'^(ssn|social[_ ]?security|national[_ ]?id)$', "id_hash_numeric"),
    (r'^(lat|latitude)$', "lat"),
    (r'^(lon|long|longitude)$', "lon"),
    (r'^(notes?|comments?|free[_ ]?text|remark|description)$', "redact"),
    (r'.*date.*', "date_shift"),
    (r'.*time.*', "datetime_shift"),
]

def _sha(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()
def _quote(name: str) -> str: return '"' + name.replace('"','""') + '"'

def _numhash(v, digits=8):
    h = _sha(_SALT + str(v)); n = int(h[:16], 16)
    return str(n % (10**digits)).zfill(digits)

def _alnhash(v, length=12): return _sha(_SALT + str(v))[:length]
def _first(v): return f"FN_{_alnhash(v,6).upper()}"
def _last(v):  return f"LN_{_alnhash(v,6).upper()}"
def _full(v):  return f"Person_{_alnhash(v,8).upper()}"
def _email(v): return f"user_{_alnhash(v,10).lower()}@example.com"
def _phone(v): d=_numhash(v,7); return f"555{d[:3]}{d[3:]}"
def _addr(v):  return f"{int(_numhash(v,3))} Anon St"
def _city(_):  return "Anonytown"
def _state(_): return "AA"
def _zip(v):   return _numhash(v,5)

def _parsedt(s):
    if s is None or isinstance(s,(int,float)): return None
    s = str(s).strip()
    if not s: return None
    for f in ("%Y-%m-%d","%Y/%m/%d","%m/%d/%Y","%d/%m/%Y",
              "%Y-%m-%d %H:%M:%S","%Y/%m/%d %H:%M:%S","%m/%d/%Y %H:%M",
              "%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M:%S.%f"):
        try: return datetime.datetime.strptime(s, f)
        except Exception: pass
    return None

def _shift(s, days, with_time):
    dt = _parsedt(s)
    if not dt: return s
    nd = dt + datetime.timedelta(days=days)
    return nd.strftime("%Y-%m-%d %H:%M:%S" if with_time else "%Y-%m-%d")

def _dob(s):
    dt = _parsedt(s)
    return f"{dt.year}-01-01" if dt else s

def _classify(col: str):
    c = col.lower().strip()
    for pat, kind in _PATTERNS:
        if re.match(pat, c): return kind
    return None

def _find_local_db(case_dir: Path, norm_id: str) -> Path | None:
    root = case_dir / f"{norm_id} TDC Sessions"
    if not root.exists(): return None
    cands = [p for p in root.rglob("local.db") if p.is_file()]
    if not cands: return None
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]

def anonymize_in_place(
    db_path: Path,
    date_shift_days: int = 137,
    make_temp_proof: bool = True,
    logger: logging.Logger | None = None,
) -> dict:
    log = logger or logging.getLogger(__name__)
    if make_temp_proof:
        with tempfile.TemporaryDirectory() as td:
            shutil.copy2(db_path, Path(td) / "local.db.backup")
        log.debug("Temporary proof copy created for %s", db_path)
    con = sqlite3.connect(db_path); con.row_factory = sqlite3.Row
    cur = con.cursor(); cur.execute("PRAGMA foreign_keys=OFF")

    TRANS = {
        "id_hash_numeric": lambda v: None if v is None else _numhash(v, digits=min(12, max(6, len(str(v)) if isinstance(v, str) else 8))),
        "id_hash_alnum":   lambda v: None if v is None else _alnhash(v, length=12),
        "id_passthrough":  lambda v: v,
        "first_name":      lambda v: None if v is None else _first(v),
        "last_name":       lambda v: None if v is None else _last(v),
        "full_name":       lambda v: None if v is None else _full(v),
        "email":           lambda v: None if v is None else _email(v),
        "phone":           lambda v: None if v is None else _phone(v),
        "address":         lambda v: None if v is None else _addr(v),
        "city":            lambda v: None if v is None else _city(v),
        "state":           lambda v: None if v is None else _state(v),
        "zip":             lambda v: None if v is None else _zip(v),
        "dob":             lambda v: None if v is None else _dob(v),
        "lat":             lambda v: None,
        "lon":             lambda v: None,
        "redact":          lambda v: None if v is None else "[REDACTED]",
        "date_shift":      lambda v: None if v is None else _shift(v, date_shift_days, False),
        "datetime_shift":  lambda v: None if v is None else _shift(v, date_shift_days, True),
    }

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [r[0] for r in cur.fetchall()]
    summary = {"db": str(db_path), "tables": [], "columns": {}}

    for t in tables:
        cur.execute(f"PRAGMA table_info({_quote(t)})")
        info = cur.fetchall()
        cols = [r["name"] for r in info]
        pks  = [r["name"] for r in info if r["pk"]]
        kinds = {c: _classify(c) for c in cols}
        summary["columns"][t] = kinds

        has_rowid = True
        try:
            cur.execute(f"SELECT rowid AS __rid__, * FROM {_quote(t)}")
            rows = cur.fetchall()
        except sqlite3.OperationalError:
            has_rowid = False
            cur.execute(f"SELECT * FROM {_quote(t)}")
            rows = cur.fetchall()

        updated = 0
        if rows:
            if has_rowid:
                set_cols = [c for c in cols if kinds[c] and kinds[c] in TRANS]
                if set_cols:
                    set_sql = ", ".join([f"{_quote(c)}=?" for c in set_cols])
                    upd = f"UPDATE {_quote(t)} SET {set_sql} WHERE rowid=?"
                    for r in rows:
                        vals = [TRANS[kinds[c]](r[c]) for c in set_cols]
                        cur.execute(upd, (*vals, r["__rid__"])); updated += 1
            else:
                if pks:
                    set_cols = [c for c in cols if c not in pks and kinds[c] and kinds[c] in TRANS]
                    if set_cols:
                        set_sql = ", ".join([f"{_quote(c)}=?" for c in set_cols])
                        where = " AND ".join([f"{_quote(c)}=?" for c in pks])
                        upd = f"UPDATE {_quote(t)} SET {set_sql} WHERE {where}"
                        for r in rows:
                            vals = [TRANS[kinds[c]](r[c]) for c in set_cols]
                            keys = [r[c] for c in pks]
                            cur.execute(upd, (*vals, *keys)); updated += 1

        con.commit()
        cur.execute(f"SELECT COUNT(*) FROM {_quote(t)}"); n = cur.fetchone()[0]
        summary["tables"].append({"table": t, "rows": n, "updated_rows": updated})

    con.execute("PRAGMA foreign_keys=ON"); con.commit(); con.close()
    return summary

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--case-dir", help="Canonical case directory root")
    g.add_argument("--db", help="Path to local.db")
    ap.add_argument("--norm-id", help="NNN_NN-NNN if using --case-dir")
    ap.add_argument("--date-shift-days", type=int, default=137)
    ap.add_argument("--no-temp-proof", dest="no_temp_proof", action="store_true",
                    help="Do not make a temp backup copy")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    log = logging.getLogger(__name__)

    if args.db:
        db_path = Path(args.db)
    else:
        if not args.norm_id:
            ap.error("--norm-id required with --case-dir")
        db_path = _find_local_db(Path(args.case_dir), args.norm_id)
        if not db_path:
            sys.stdout.write(json.dumps({"ok": False, "error": "local.db not found"}) + "\n")
            return 2

    s = anonymize_in_place(
        db_path,
        args.date_shift_days,
        make_temp_proof=(not args.no_temp_proof),
        logger=log,
    )
    sys.stdout.write(json.dumps({"ok": True, "summary": s}, indent=2) + "\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
