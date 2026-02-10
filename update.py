#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import random
import re
import shutil
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Set

import pandas as pd
import requests
import xml.etree.ElementTree as ET

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# -----------------------------
# Constants / dataset selection
# -----------------------------
DATASETS = {
    "netzanschlusspunkte": "Netzanschlusspunkte_",
    "lokationen": "Lokationen_",
    "einheiten_solar": "EinheitenSolar_",
    "einheiten_stromspeicher": "EinheitenStromSpeicher_",
}

DEFAULT_DOWNLOAD_PAGE = "https://www.marktstammdatenregister.de/MaStR/Datendownload"

TABLE_RUNS = "MASTR_EXPORT_RUNS"

STG_NETZ = "STG_NETZANSCHLUSSPUNKTE"
STG_LOK = "STG_LOKATIONEN"
STG_SOLAR = "STG_EINHEITEN_SOLAR"
STG_BAT = "STG_EINHEITEN_STROMSPEICHER"

CUR_NETZ = "CUR_NETZANSCHLUSSPUNKTE"
CUR_LOK = "CUR_LOKATIONEN"
CUR_SOLAR = "CUR_EINHEITEN_SOLAR"
CUR_BAT = "CUR_EINHEITEN_STROMSPEICHER"

DEFAULT_SUMMARY_TABLE = "MASTR_MELO_SUMMARY"
DEFAULT_CUSTOMER_MATCH_VIEW = "VW_CUSTOMER_MASTR_MATCH"
DEFAULT_DIAG_VIEW = "VW_MASTR_SEL_WITHOUT_MELO"
DEFAULT_CUSTOMER_TABLE_FQN = "MSB_SANDBOX.EXPOSED.MD_CUSTOMER_REGISTER"


# -----------------------------
# Logging
# -----------------------------
def setup_logging(log_path: Path, level: str = "INFO", file_mode: str = "a") -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_path, mode=file_mode, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)


# -----------------------------
# Generic helpers
# -----------------------------
def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def fqn(db: str, schema: str, name: str) -> str:
    return f"{db}.{schema}.{name}"

def extract_export_date_from_url(url: str) -> Optional[str]:
    m = re.search(r"Gesamtdatenexport_(\d{8})_", url or "")
    return m.group(1) if m else None

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def record_hash_from_values(values: List[Optional[str]]) -> str:
    norm = [(v or "").strip() for v in values]
    return sha256_text("\u001f".join(norm))

def is_valid_zip(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    try:
        with zipfile.ZipFile(path, "r") as z:
            bad = z.testzip()
            return bad is None
    except Exception:
        return False


# -----------------------------
# .env loader
# -----------------------------
def load_env_file(env_path: Path, override: bool = False) -> None:
    if not env_path or not env_path.exists():
        return

    text = env_path.read_text(encoding="utf-8", errors="ignore")
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if not k:
            continue
        if override or (k not in os.environ):
            os.environ[k] = v

def missing_required_env(required: List[str]) -> List[str]:
    missing = []
    for k in required:
        v = os.getenv(k)
        if v is None or str(v).strip() == "":
            missing.append(k)
    return missing


# -----------------------------
# Download discovery + download
# -----------------------------
def discover_latest_zip_url(download_page_url: str) -> str:
    logging.info("Discovering latest ZIP URL from: %s", download_page_url)
    r = requests.get(download_page_url, timeout=60)
    r.raise_for_status()

    urls = set(re.findall(r"https?://[^\s\"']+\.zip", r.text, flags=re.IGNORECASE))
    if not urls:
        raise RuntimeError("No .zip URLs found on the download page. Use --download-url.")

    scored: List[Tuple[str, str]] = []
    for u in urls:
        d = extract_export_date_from_url(u) or "00000000"
        scored.append((d, u))
    scored.sort(reverse=True)
    best = scored[0][1]
    logging.info("Discovered ZIP URL: %s", best)
    return best

def download_file(url: str, out_path: Path, progress: bool = True) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("Downloading ZIP: %s", url)

    if out_path.exists():
        out_path.unlink()

    pbar = None
    tqdm = None
    if progress:
        try:
            from tqdm import tqdm as _tqdm
            tqdm = _tqdm
        except Exception:
            tqdm = None

    try:
        with requests.get(url, stream=True, timeout=180) as r:
            r.raise_for_status()

            total_header = r.headers.get("Content-Length")
            total = int(total_header) if total_header and total_header.isdigit() else None

            if tqdm is not None:
                pbar = tqdm(total=total, unit="B", unit_scale=True, desc="Download ZIP")

            with out_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    if pbar is not None:
                        pbar.update(len(chunk))

            if pbar is not None:
                pbar.close()

        logging.info("Saved ZIP to %s (%d bytes)", out_path, out_path.stat().st_size)

    except Exception:
        if pbar is not None:
            try:
                pbar.close()
            except Exception:
                pass
        if out_path.exists():
            try:
                out_path.unlink()
            except Exception:
                pass
        raise


# -----------------------------
# Cache (ZIP) behavior
# -----------------------------
def prune_old_cached_zips(cache_dir: Path, keep_zip: Path) -> List[Path]:
    deleted: List[Path] = []
    cache_dir.mkdir(parents=True, exist_ok=True)

    try:
        keep_resolved = keep_zip.resolve()
    except Exception:
        keep_resolved = keep_zip

    for p in cache_dir.glob("mastr_export_*.zip"):
        try:
            p_resolved = p.resolve()
        except Exception:
            p_resolved = p

        if p_resolved == keep_resolved:
            continue
        if not p.is_file():
            continue

        try:
            size = p.stat().st_size
        except Exception:
            size = None

        try:
            p.unlink()
            deleted.append(p)
            logging.info("Pruned old cached ZIP: %s (size_bytes=%s)", p, size)
        except Exception as e:
            logging.warning("Could not delete old cached ZIP %s: %s", p, e)

    return deleted

def ensure_zip_available(
    *,
    download_url: str,
    export_date: str,
    cache_dir: Path,
    force_download: bool,
    progress: bool,
    prune_old_zips: bool,
) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_zip = cache_dir / f"mastr_export_{export_date}.zip"

    if not force_download and is_valid_zip(cache_zip):
        logging.info("Reusing cached ZIP: %s", cache_zip)
        if prune_old_zips:
            prune_old_cached_zips(cache_dir, keep_zip=cache_zip)
        return cache_zip

    logging.info("No valid cached ZIP found (or --force-download). Downloading fresh ZIP into cache.")
    download_file(download_url, cache_zip, progress=progress)

    if not is_valid_zip(cache_zip):
        raise RuntimeError(f"Downloaded ZIP is not valid: {cache_zip}")

    if prune_old_zips:
        prune_old_cached_zips(cache_dir, keep_zip=cache_zip)

    return cache_zip


# -----------------------------
# Robust cleanup (Windows)
# -----------------------------
def _rmtree_onerror(func, path, exc_info):
    try:
        os.chmod(path, 0o700)
    except Exception:
        pass
    try:
        func(path)
    except Exception:
        pass

def rmtree_with_retries(path: Path, retries: int = 6, sleep_s: float = 2.0) -> None:
    if not path.exists():
        return
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            shutil.rmtree(path, onerror=_rmtree_onerror)
            return
        except Exception as e:
            last_err = e
            logging.warning("rmtree failed (%d/%d) for %s: %s", i + 1, retries, path, e)
            time.sleep(sleep_s)
    raise last_err  # type: ignore[misc]


# -----------------------------
# Extraction (TEMP only)
# -----------------------------
def extract_selected_xml_to_temp(
    *,
    zip_path: Path,
    temp_dir: Path,
    progress: bool = True,
) -> Dict[str, List[Path]]:
    if temp_dir.exists():
        rmtree_with_retries(temp_dir, retries=6, sleep_s=2.0)
    temp_dir.mkdir(parents=True, exist_ok=True)

    extracted: Dict[str, List[Path]] = {k: [] for k in DATASETS.keys()}

    logging.info("Extracting selected XML files to TEMP dir: %s", temp_dir)

    tqdm = None
    if progress:
        try:
            from tqdm import tqdm as _tqdm
            tqdm = _tqdm
        except Exception:
            tqdm = None

    with zipfile.ZipFile(zip_path, "r") as z:
        members = z.namelist()
        member_iter = tqdm(members, desc="Extract ZIP members", unit="file") if tqdm is not None else members

        for m in member_iter:
            base = Path(m).name
            if not base.lower().endswith(".xml"):
                continue

            for dataset_key, prefix in DATASETS.items():
                if base.startswith(prefix):
                    out_folder = temp_dir / dataset_key
                    out_folder.mkdir(parents=True, exist_ok=True)
                    out_path = out_folder / base
                    if not out_path.exists():
                        with z.open(m) as src, out_path.open("wb") as dst:
                            shutil.copyfileobj(src, dst, length=1024 * 1024)
                    extracted[dataset_key].append(out_path)
                    break

    for k in extracted:
        extracted[k] = sorted(extracted[k])
        logging.info("Extracted %-24s : %d files", k, len(extracted[k]))

    return extracted


# -----------------------------
# XML parsing
# -----------------------------
def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def infer_record_tag(xml_path: Path, max_events: int = 250_000) -> str:
    counts: Dict[str, int] = {}
    depth = 0
    events = 0

    with xml_path.open("rb") as f:
        for event, elem in ET.iterparse(f, events=("start", "end")):
            events += 1
            if event == "start":
                depth += 1
                if depth == 2:
                    t = strip_ns(elem.tag)
                    counts[t] = counts.get(t, 0) + 1
            else:
                depth -= 1
                elem.clear()

            if events >= max_events and counts:
                break

    if not counts:
        raise ValueError(f"Could not infer record_tag for {xml_path.name}")

    return max(counts.items(), key=lambda kv: kv[1])[0]

def iter_records(xml_path: Path, record_tag: str, wanted_tags: Optional[Set[str]] = None) -> Iterator[Dict[str, str]]:
    wanted = set(wanted_tags) if wanted_tags else None

    with xml_path.open("rb") as f:
        for _, elem in ET.iterparse(f, events=("end",)):
            if strip_ns(elem.tag) == record_tag:
                rec: Dict[str, str] = {}
                for child in elem:
                    k = strip_ns(child.tag)
                    if wanted is not None and k not in wanted:
                        continue
                    v = (child.text or "").strip()
                    if k in rec and v:
                        rec[k] = rec[k] + "|" + v
                    elif k not in rec:
                        rec[k] = v
                yield rec
                elem.clear()


# -----------------------------
# Snowflake connection + SQL
# -----------------------------
@dataclass
class SnowflakeCfg:
    account: str
    user: str
    role: str
    warehouse: str
    database: str
    schema: str
    auth: str  # externalbrowser | jwt
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None

def connect_snowflake(cfg: SnowflakeCfg):
    logging.info("Connecting to Snowflake (auth=%s)", cfg.auth)

    base_kwargs = dict(
        account=cfg.account,
        user=cfg.user,
        role=cfg.role,
        warehouse=cfg.warehouse,
        database=cfg.database,
        schema=cfg.schema,
    )

    if cfg.auth == "externalbrowser":
        return snowflake.connector.connect(**base_kwargs, authenticator="externalbrowser")

    if cfg.auth == "jwt":
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        if not cfg.private_key_path:
            raise ValueError("JWT auth requires --private-key-path")

        key_bytes = Path(cfg.private_key_path).read_bytes()
        passphrase = cfg.private_key_passphrase.encode("utf-8") if cfg.private_key_passphrase else None

        pkey = serialization.load_pem_private_key(key_bytes, password=passphrase, backend=default_backend())
        private_key_der = pkey.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return snowflake.connector.connect(
            **base_kwargs,
            authenticator="SNOWFLAKE_JWT",
            private_key=private_key_der,
        )

    raise ValueError(f"Unknown auth mode: {cfg.auth}")

def run_sql(conn, sql: str, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params) if params else cur.execute(sql)
        try:
            return cur.fetchall()
        except Exception:
            return None
    finally:
        cur.close()


# -----------------------------
# Schema management
# -----------------------------
def get_existing_columns(conn, db: str, schema: str, table: str) -> set[str]:
    rows = run_sql(
        conn,
        f"""
        SELECT COLUMN_NAME
        FROM {db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        """,
        (schema.upper(), table.upper()),
    )
    return {r[0].upper() for r in rows} if rows else set()

def ensure_table_with_columns(
    conn,
    db: str,
    schema: str,
    table: str,
    columns: List[Tuple[str, str]],
) -> None:
    if not columns:
        raise ValueError("columns must not be empty")

    col_ddl = ",\n  ".join([f"{name} {typ}" for name, typ in columns])
    run_sql(conn, f"CREATE TABLE IF NOT EXISTS {fqn(db, schema, table)} (\n  {col_ddl}\n)")

    existing = get_existing_columns(conn, db, schema, table)
    for name, typ in columns:
        if name.upper() not in existing:
            logging.info("Altering table %s: add missing column %s %s", fqn(db, schema, table), name, typ)
            run_sql(conn, f"ALTER TABLE {fqn(db, schema, table)} ADD COLUMN {name} {typ}")

def ensure_tables(conn, cfg: SnowflakeCfg) -> None:
    db, sch = cfg.database, cfg.schema

    ensure_table_with_columns(
        conn, db, sch, TABLE_RUNS,
        columns=[
            ("RUN_ID", "STRING"),
            ("FETCH_TS_UTC", "TIMESTAMP_TZ"),
            ("DOWNLOAD_PAGE_URL", "STRING"),
            ("DOWNLOAD_URL", "STRING"),
            ("LOCAL_FILENAME", "STRING"),
            ("FILE_SIZE_BYTES", "NUMBER"),
            ("SHA256", "STRING"),
            ("EXPORT_DATE", "STRING"),
            ("STATUS", "STRING"),
            ("ERROR_MESSAGE", "STRING"),
        ],
    )

    stg_common = [
        ("RUN_ID", "STRING"),
        ("EXPORT_DATE", "STRING"),
        ("SOURCE_FILE", "STRING"),
        ("RECORD_HASH", "STRING"),
    ]

    ensure_table_with_columns(
        conn, db, sch, STG_NETZ,
        columns=stg_common + [
            ("NETZANSCHLUSSPUNKT_MASTR_NUMMER", "STRING"),
            ("LOKATION_MASTR_NUMMER", "STRING"),
            ("MELO", "STRING"),
        ],
    )
    ensure_table_with_columns(
        conn, db, sch, STG_LOK,
        columns=stg_common + [
            ("LOKATION_MASTR_NUMMER", "STRING"),
            ("VERKNUEPFTE_EINHEITEN_MASTR_NUMMERN", "STRING"),
            ("NETZANSCHLUSSPUNKTE_MASTR_NUMMERN", "STRING"),
        ],
    )
    ensure_table_with_columns(
        conn, db, sch, STG_SOLAR,
        columns=stg_common + [
            ("EINHEIT_MASTR_NUMMER", "STRING"),
            ("LOKATION_MASTR_NUMMER", "STRING"),
            ("POSTLEITZAHL", "STRING"),
            ("ORT", "STRING"),
            ("REGISTRIERUNGSDATUM", "STRING"),
            ("INBETRIEBNAHMEDATUM", "STRING"),
            ("BRUTTOLEISTUNG_RAW", "STRING"),
            ("NETTONENNLEISTUNG_RAW", "STRING"),
            ("ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW", "STRING"),
            ("SPEICHERAMGLEICHENORT", "STRING"),
            ("NAMESTROMERZEUGUNGSEINHEIT", "STRING"),
        ],
    )
    ensure_table_with_columns(
        conn, db, sch, STG_BAT,
        columns=stg_common + [
            ("EINHEIT_MASTR_NUMMER", "STRING"),
            ("LOKATION_MASTR_NUMMER", "STRING"),
            ("POSTLEITZAHL", "STRING"),
            ("ORT", "STRING"),
            ("REGISTRIERUNGSDATUM", "STRING"),
            ("INBETRIEBNAHMEDATUM", "STRING"),
            ("BRUTTOLEISTUNG_RAW", "STRING"),
            ("NETTONENNLEISTUNG_RAW", "STRING"),
            ("ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW", "STRING"),
            ("EEGMASTRNUMMER", "STRING"),
            ("NAMESTROMERZEUGUNGSEINHEIT", "STRING"),
        ],
    )

    cur_common = [
        ("RECORD_HASH", "STRING"),
        ("EXPORT_DATE", "STRING"),
        ("SOURCE_FILE", "STRING"),
        ("RUN_ID", "STRING"),
        ("CREATED_AT_UTC", "TIMESTAMP_TZ"),
        ("UPDATED_AT_UTC", "TIMESTAMP_TZ"),
    ]

    ensure_table_with_columns(
        conn, db, sch, CUR_NETZ,
        columns=[("NETZANSCHLUSSPUNKT_MASTR_NUMMER", "STRING")] + cur_common + [
            ("LOKATION_MASTR_NUMMER", "STRING"),
            ("MELO", "STRING"),
        ],
    )
    ensure_table_with_columns(
        conn, db, sch, CUR_LOK,
        columns=[("LOKATION_MASTR_NUMMER", "STRING")] + cur_common + [
            ("VERKNUEPFTE_EINHEITEN_MASTR_NUMMERN", "STRING"),
            ("NETZANSCHLUSSPUNKTE_MASTR_NUMMERN", "STRING"),
        ],
    )
    ensure_table_with_columns(
        conn, db, sch, CUR_SOLAR,
        columns=[("EINHEIT_MASTR_NUMMER", "STRING")] + cur_common + [
            ("LOKATION_MASTR_NUMMER", "STRING"),
            ("POSTLEITZAHL", "STRING"),
            ("ORT", "STRING"),
            ("REGISTRIERUNGSDATUM", "STRING"),
            ("INBETRIEBNAHMEDATUM", "STRING"),
            ("BRUTTOLEISTUNG_RAW", "STRING"),
            ("NETTONENNLEISTUNG_RAW", "STRING"),
            ("ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW", "STRING"),
            ("SPEICHERAMGLEICHENORT", "STRING"),
            ("NAMESTROMERZEUGUNGSEINHEIT", "STRING"),
        ],
    )
    ensure_table_with_columns(
        conn, db, sch, CUR_BAT,
        columns=[("EINHEIT_MASTR_NUMMER", "STRING")] + cur_common + [
            ("LOKATION_MASTR_NUMMER", "STRING"),
            ("POSTLEITZAHL", "STRING"),
            ("ORT", "STRING"),
            ("REGISTRIERUNGSDATUM", "STRING"),
            ("INBETRIEBNAHMEDATUM", "STRING"),
            ("BRUTTOLEISTUNG_RAW", "STRING"),
            ("NETTONENNLEISTUNG_RAW", "STRING"),
            ("ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW", "STRING"),
            ("EEGMASTRNUMMER", "STRING"),
            ("NAMESTROMERZEUGUNGSEINHEIT", "STRING"),
        ],
    )


# -----------------------------
# Run logging table
# -----------------------------
def log_run_start(conn, cfg: SnowflakeCfg, meta: dict) -> None:
    run_sql(conn, f"""
    INSERT INTO {fqn(cfg.database, cfg.schema, TABLE_RUNS)}
    (RUN_ID, FETCH_TS_UTC, DOWNLOAD_PAGE_URL, DOWNLOAD_URL, LOCAL_FILENAME, FILE_SIZE_BYTES, SHA256, EXPORT_DATE, STATUS, ERROR_MESSAGE)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        meta["run_id"],
        meta["fetch_ts_utc"],
        meta["download_page_url"],
        meta["download_url"],
        meta["local_filename"],
        meta["size_bytes"],
        meta["sha256"],
        meta.get("export_date"),
        "STARTED",
        None,
    ))

def log_run_end(conn, cfg: SnowflakeCfg, run_id: str, status: str, error_message: Optional[str]) -> None:
    run_sql(conn, f"""
    UPDATE {fqn(cfg.database, cfg.schema, TABLE_RUNS)}
    SET STATUS=%s, ERROR_MESSAGE=%s
    WHERE RUN_ID=%s
    """, (status, error_message, run_id))


# -----------------------------
# write_pandas wrapper
# -----------------------------
def load_df_append(conn, cfg: SnowflakeCfg, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df.columns = [c.upper() for c in df.columns]

    success, _, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table,
        database=cfg.database,
        schema=cfg.schema,
        auto_create_table=False,
        overwrite=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for {table}")
    return int(nrows)

def pick(rec: Dict[str, str], *keys: str) -> Optional[str]:
    for k in keys:
        v = rec.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return None


# -----------------------------
# Stage load (XML -> STG tables)
# -----------------------------
def _progress_rows(it, desc: str, enabled: bool):
    if not enabled:
        for x in it:
            yield x
        return

    try:
        from tqdm import tqdm
        p = tqdm(it, desc=desc, unit="rows")
        for x in p:
            yield x
        p.close()
    except Exception:
        for x in it:
            yield x

def load_xml_files_to_stage(
    *,
    conn,
    cfg: SnowflakeCfg,
    run_id: str,
    export_date: str,
    dataset_name: str,
    files: List[Path],
    stg_table: str,
    key_col: str,
    record_mapper,
    wanted_tags: Optional[Set[str]],
    progress: bool,
) -> Dict[str, int]:
    if not files:
        logging.warning("[%s] No files found. Skipping.", dataset_name)
        return {"loaded_rows": 0, "skipped_missing_key": 0}

    logging.info("[%s] TRUNCATE stage table: %s", dataset_name, fqn(cfg.database, cfg.schema, stg_table))
    run_sql(conn, f"TRUNCATE TABLE {fqn(cfg.database, cfg.schema, stg_table)}")

    record_tag = infer_record_tag(files[0])
    logging.info("[%s] Inferred record_tag=%s (from %s)", dataset_name, record_tag, files[0].name)

    total_loaded = 0
    skipped_missing_key = 0

    batch: List[dict] = []
    BATCH_SIZE = 150_000

    file_iter = files
    if progress:
        try:
            from tqdm import tqdm
            file_iter = tqdm(files, desc=f"{dataset_name}: files", unit="file")
        except Exception:
            file_iter = files

    key_col_u = key_col.upper()

    for xml_path in file_iter:
        row_iter = iter_records(xml_path, record_tag=record_tag, wanted_tags=wanted_tags)
        row_iter = _progress_rows(row_iter, desc=f"{dataset_name}: {xml_path.name}", enabled=progress)

        kept_rows_in_file = 0
        for rec in row_iter:
            mapped = record_mapper(rec, xml_path)
            key_val = mapped.get(key_col_u)

            if key_val is None or str(key_val).strip() == "":
                skipped_missing_key += 1
                continue

            batch.append(mapped)
            kept_rows_in_file += 1

            if len(batch) >= BATCH_SIZE:
                n = load_df_append(conn, cfg, stg_table, pd.DataFrame(batch))
                total_loaded += n
                logging.info("[%s] flushed stage batch: +%d rows (stage total=%d)", dataset_name, n, total_loaded)
                batch = []

        logging.info("[%s] finished file %s (kept rows=%d)", dataset_name, xml_path.name, kept_rows_in_file)

    if batch:
        n = load_df_append(conn, cfg, stg_table, pd.DataFrame(batch))
        total_loaded += n
        logging.info("[%s] final stage flush: +%d rows (stage total=%d)", dataset_name, n, total_loaded)

    logging.info("[%s] STAGE LOAD DONE loaded_rows=%d skipped_missing_key=%d", dataset_name, total_loaded, skipped_missing_key)
    return {"loaded_rows": total_loaded, "skipped_missing_key": skipped_missing_key}


# -----------------------------
# MERGE (STG -> CUR)
# -----------------------------
def merge_stage_into_current(
    *,
    conn,
    cfg: SnowflakeCfg,
    dataset_name: str,
    stg_table: str,
    cur_table: str,
    key_col: str,
    stage_cols: List[str],
    touch_unchanged: bool,
) -> None:
    db, sch = cfg.database, cfg.schema
    stg_fqn = fqn(db, sch, stg_table)
    cur_fqn = fqn(db, sch, cur_table)

    src_cols = [c.upper() for c in stage_cols]
    key_col = key_col.upper()

    update_cols = [c for c in src_cols if c != key_col]
    update_set = ",\n          ".join(
        [f"t.{c} = s.{c}" for c in update_cols] + ["t.UPDATED_AT_UTC = CURRENT_TIMESTAMP()"]
    )

    insert_cols = [key_col] + update_cols + ["CREATED_AT_UTC", "UPDATED_AT_UTC"]
    insert_col_list = ", ".join(insert_cols)
    insert_values = ", ".join([f"s.{c}" for c in [key_col] + update_cols] + ["CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()"])

    using_subquery = f"""
        SELECT {", ".join(src_cols)}
        FROM (
            SELECT
                {", ".join(src_cols)},
                ROW_NUMBER() OVER (PARTITION BY {key_col} ORDER BY SOURCE_FILE DESC) AS RN
            FROM {stg_fqn}
            WHERE {key_col} IS NOT NULL AND TRIM({key_col}) <> ''
        )
        WHERE RN = 1
    """

    matched_condition = "1=1" if touch_unchanged else "COALESCE(t.RECORD_HASH,'') <> COALESCE(s.RECORD_HASH,'')"

    merge_sql = f"""
    MERGE INTO {cur_fqn} AS t
    USING ({using_subquery}) AS s
      ON t.{key_col} = s.{key_col}
    WHEN MATCHED AND {matched_condition} THEN
      UPDATE SET
          {update_set}
    WHEN NOT MATCHED THEN
      INSERT ({insert_col_list})
      VALUES ({insert_values})
    """

    logging.info("[%s] MERGE %s <- %s", dataset_name, cur_fqn, stg_fqn)
    run_sql(conn, merge_sql)
    logging.info("[%s] MERGE executed.", dataset_name)


# -----------------------------
# Wanted tags + mappers
# -----------------------------
WANTED_TAGS_NETZ = {
    "NetzanschlusspunktMastrNummer", "NetzanschlusspunktMaStRNummer",
    "LokationMaStRNummer", "LokationMastrNummer",
    "Messlokation", "MELO", "MeteringCode",
}
WANTED_TAGS_LOK = {
    "MastrNummer", "MaStRNummer", "LokationMaStRNummer", "LokationMastrNummer",
    "VerknuepfteEinheitenMaStRNummern",
    "NetzanschlusspunkteMaStRNummern",
}
WANTED_TAGS_SOLAR = {
    "EinheitMastrNummer", "EinheitMaStRNummer",
    "LokationMaStRNummer", "LokationMastrNummer",
    "Postleitzahl", "PLZ",
    "Ort", "Stadt", "Gemeinde",
    "Registrierungsdatum",
    "Inbetriebnahmedatum",
    "Bruttoleistung",
    "Nettonennleistung",
    "ZugeordneteWirkleistungWechselrichter",
    "ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER",
    "SpeicherAmGleichenOrt",
    "SPEICHERAMGLEICHENORT",
    "NameStromerzeugungseinheit",
    "NAMESTROMERZEUGUNGSEINHEIT",
}
WANTED_TAGS_BAT = set(WANTED_TAGS_SOLAR) | {"EEGMastrNummer", "EEGMaStRNummer", "EEGMASTRNUMMER"}

def map_netz(rec: Dict[str, str], xml_path: Path, run_id: str, export_date: str) -> dict:
    out = {
        "RUN_ID": run_id,
        "EXPORT_DATE": export_date,
        "SOURCE_FILE": xml_path.name,
        "NETZANSCHLUSSPUNKT_MASTR_NUMMER": pick(rec, "NetzanschlusspunktMastrNummer", "NetzanschlusspunktMaStRNummer"),
        "LOKATION_MASTR_NUMMER": pick(rec, "LokationMaStRNummer", "LokationMastrNummer"),
        "MELO": pick(rec, "Messlokation", "MELO", "MeteringCode"),
    }
    out["RECORD_HASH"] = record_hash_from_values([out.get("NETZANSCHLUSSPUNKT_MASTR_NUMMER"), out.get("LOKATION_MASTR_NUMMER"), out.get("MELO")])
    return out

def map_lok(rec: Dict[str, str], xml_path: Path, run_id: str, export_date: str) -> dict:
    out = {
        "RUN_ID": run_id,
        "EXPORT_DATE": export_date,
        "SOURCE_FILE": xml_path.name,
        "LOKATION_MASTR_NUMMER": pick(rec, "MastrNummer", "MaStRNummer", "LokationMaStRNummer", "LokationMastrNummer"),
        "VERKNUEPFTE_EINHEITEN_MASTR_NUMMERN": pick(rec, "VerknuepfteEinheitenMaStRNummern"),
        "NETZANSCHLUSSPUNKTE_MASTR_NUMMERN": pick(rec, "NetzanschlusspunkteMaStRNummern"),
    }
    out["RECORD_HASH"] = record_hash_from_values([out.get("LOKATION_MASTR_NUMMER"), out.get("VERKNUEPFTE_EINHEITEN_MASTR_NUMMERN"), out.get("NETZANSCHLUSSPUNKTE_MASTR_NUMMERN")])
    return out

def map_solar(rec: Dict[str, str], xml_path: Path, run_id: str, export_date: str) -> dict:
    out = {
        "RUN_ID": run_id,
        "EXPORT_DATE": export_date,
        "SOURCE_FILE": xml_path.name,
        "EINHEIT_MASTR_NUMMER": pick(rec, "EinheitMastrNummer", "EinheitMaStRNummer"),
        "LOKATION_MASTR_NUMMER": pick(rec, "LokationMaStRNummer", "LokationMastrNummer"),
        "POSTLEITZAHL": pick(rec, "Postleitzahl", "PLZ"),
        "ORT": pick(rec, "Ort", "Stadt", "Gemeinde"),
        "REGISTRIERUNGSDATUM": pick(rec, "Registrierungsdatum"),
        "INBETRIEBNAHMEDATUM": pick(rec, "Inbetriebnahmedatum"),
        "BRUTTOLEISTUNG_RAW": pick(rec, "Bruttoleistung"),
        "NETTONENNLEISTUNG_RAW": pick(rec, "Nettonennleistung"),
        "ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW": pick(rec, "ZugeordneteWirkleistungWechselrichter", "ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER"),
        "SPEICHERAMGLEICHENORT": pick(rec, "SpeicherAmGleichenOrt", "SPEICHERAMGLEICHENORT"),
        "NAMESTROMERZEUGUNGSEINHEIT": pick(rec, "NameStromerzeugungseinheit", "NAMESTROMERZEUGUNGSEINHEIT"),
    }
    out["RECORD_HASH"] = record_hash_from_values([
        out.get("EINHEIT_MASTR_NUMMER"), out.get("LOKATION_MASTR_NUMMER"),
        out.get("POSTLEITZAHL"), out.get("ORT"),
        out.get("REGISTRIERUNGSDATUM"), out.get("INBETRIEBNAHMEDATUM"),
        out.get("BRUTTOLEISTUNG_RAW"), out.get("NETTONENNLEISTUNG_RAW"),
        out.get("ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW"),
        out.get("SPEICHERAMGLEICHENORT"),
        out.get("NAMESTROMERZEUGUNGSEINHEIT"),
    ])
    return out

def map_battery(rec: Dict[str, str], xml_path: Path, run_id: str, export_date: str) -> dict:
    out = {
        "RUN_ID": run_id,
        "EXPORT_DATE": export_date,
        "SOURCE_FILE": xml_path.name,
        "EINHEIT_MASTR_NUMMER": pick(rec, "EinheitMastrNummer", "EinheitMaStRNummer"),
        "LOKATION_MASTR_NUMMER": pick(rec, "LokationMaStRNummer", "LokationMastrNummer"),
        "POSTLEITZAHL": pick(rec, "Postleitzahl", "PLZ"),
        "ORT": pick(rec, "Ort", "Stadt", "Gemeinde"),
        "REGISTRIERUNGSDATUM": pick(rec, "Registrierungsdatum"),
        "INBETRIEBNAHMEDATUM": pick(rec, "Inbetriebnahmedatum"),
        "BRUTTOLEISTUNG_RAW": pick(rec, "Bruttoleistung"),
        "NETTONENNLEISTUNG_RAW": pick(rec, "Nettonennleistung"),
        "ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW": pick(
            rec,
            "ZugeordneteWirkleistungWechselrichter",
            "ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER",
        ),
        "EEGMASTRNUMMER": pick(rec, "EEGMastrNummer", "EEGMaStRNummer", "EEGMASTRNUMMER"),
        "NAMESTROMERZEUGUNGSEINHEIT": pick(rec, "NameStromerzeugungseinheit", "NAMESTROMERZEUGUNGSEINHEIT"),
    }
    out["RECORD_HASH"] = record_hash_from_values([
        out.get("EINHEIT_MASTR_NUMMER"),
        out.get("LOKATION_MASTR_NUMMER"),
        out.get("POSTLEITZAHL"),
        out.get("ORT"),
        out.get("REGISTRIERUNGSDATUM"),
        out.get("INBETRIEBNAHMEDATUM"),
        out.get("BRUTTOLEISTUNG_RAW"),
        out.get("NETTONENNLEISTUNG_RAW"),
        out.get("ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW"),
        out.get("EEGMASTRNUMMER"),
        out.get("NAMESTROMERZEUGUNGSEINHEIT"),
    ])
    return out


# -----------------------------
# Helpers: file limit
# -----------------------------
def apply_max_files_per_dataset(extracted_map: Dict[str, List[Path]], max_files: int) -> Dict[str, List[Path]]:
    if not max_files or max_files <= 0:
        return extracted_map
    limited: Dict[str, List[Path]] = {}
    for k, files in extracted_map.items():
        files_sorted = sorted(files)
        limited[k] = files_sorted[:max_files]
        logging.warning("File limit active: dataset=%s using %d/%d files (max_files_per_type=%d)", k, len(limited[k]), len(files_sorted), max_files)
    return limited


# -----------------------------
# Summary SQL
# -----------------------------
def build_summary_sql(
    *,
    db: str,
    schema: str,
    summary_table: str,
    customer_table_fqn: str,
    export_date: str,
    build_views: bool = True,
) -> List[str]:
    cur_netz = f"{db}.{schema}.{CUR_NETZ}"
    cur_solar = f"{db}.{schema}.{CUR_SOLAR}"
    cur_bat = f"{db}.{schema}.{CUR_BAT}"
    summary_fqn = f"{db}.{schema}.{summary_table}"

    stmts: List[str] = []

    # 1) MELO <-> SEL <-> SAN mapping (distinct)
    stmts.append(f"""
    CREATE OR REPLACE TEMP TABLE TMP_SEL_MELO AS
    SELECT DISTINCT
      COALESCE(LOKATION_MASTR_NUMMER, '') AS SEL,
      COALESCE(MELO, '') AS MELO,
      COALESCE(NETZANSCHLUSSPUNKT_MASTR_NUMMER, '') AS SAN
    FROM {cur_netz}
    WHERE MELO IS NOT NULL AND TRIM(MELO) <> '';
    """)

    # 2) Solar aggregation pro MELO
    stmts.append(f"""
    CREATE OR REPLACE TEMP TABLE TMP_SOLAR_AGG AS
    SELECT
      m.MELO,
      COUNT(DISTINCT s.EINHEIT_MASTR_NUMMER) AS SOLAR_UNITS,
      SUM(TRY_TO_NUMBER(s.BRUTTOLEISTUNG_RAW)) AS TOTAL_SOLAR_KWP,
      ARRAY_AGG(DISTINCT s.NAMESTROMERZEUGUNGSEINHEIT) AS SOLAR_NAMES,
      ARRAY_AGG(DISTINCT s.EINHEIT_MASTR_NUMMER) AS RAW_SOLAR_LIST
    FROM TMP_SEL_MELO m
    LEFT JOIN {cur_solar} s
      ON s.LOKATION_MASTR_NUMMER = m.SEL
    GROUP BY m.MELO;
    """)

    # 3) Storage aggregation pro MELO
    stmts.append(f"""
    CREATE OR REPLACE TEMP TABLE TMP_BAT_AGG AS
    SELECT
      m.MELO,
      COUNT(DISTINCT b.EINHEIT_MASTR_NUMMER) AS STORAGE_UNITS,
      SUM(TRY_TO_NUMBER(b.BRUTTOLEISTUNG_RAW)) AS TOTAL_STORAGE_KW,
      ARRAY_AGG(DISTINCT b.NAMESTROMERZEUGUNGSEINHEIT) AS STORAGE_NAMES,
      ARRAY_AGG(DISTINCT b.EINHEIT_MASTR_NUMMER) AS RAW_STORAGE_LIST
    FROM TMP_SEL_MELO m
    LEFT JOIN {cur_bat} b
      ON b.LOKATION_MASTR_NUMMER = m.SEL
    GROUP BY m.MELO;
    """)

    # 4) Representative SEL/SAN und Listen
    stmts.append(f"""
    CREATE OR REPLACE TEMP TABLE TMP_MELO_META AS
    SELECT
      MELO,
      ANY_VALUE(SEL) AS SEL,
      ANY_VALUE(SAN) AS SAN,
      ARRAY_AGG(DISTINCT SEL) AS SEL_LIST,
      ARRAY_AGG(DISTINCT SAN) AS SAN_LIST
    FROM TMP_SEL_MELO
    GROUP BY MELO;
    """)

    # 5) Final summary table
    stmts.append(f"""
    CREATE OR REPLACE TABLE {summary_fqn} AS
    SELECT
      meta.MELO,
      meta.SEL,
      meta.SAN,
      meta.SEL_LIST,
      meta.SAN_LIST,
      COALESCE(solar.SOLAR_UNITS, 0) AS SOLAR_UNITS,
      COALESCE(solar.TOTAL_SOLAR_KWP, 0) AS TOTAL_SOLAR_KWP,
      COALESCE(bat.STORAGE_UNITS, 0) AS STORAGE_UNITS,
      COALESCE(bat.TOTAL_STORAGE_KW, 0) AS TOTAL_STORAGE_KW,
      COALESCE(solar.SOLAR_NAMES, ARRAY_CONSTRUCT()) AS SOLAR_NAMES,
      COALESCE(bat.STORAGE_NAMES, ARRAY_CONSTRUCT()) AS STORAGE_NAMES,
      COALESCE(solar.RAW_SOLAR_LIST, ARRAY_CONSTRUCT()) AS RAW_SOLAR_LIST,
      COALESCE(bat.RAW_STORAGE_LIST, ARRAY_CONSTRUCT()) AS RAW_STORAGE_LIST,
      '{export_date}' AS EXPORT_DATE,
      CURRENT_TIMESTAMP() AS BUILT_AT_UTC
    FROM TMP_MELO_META meta
    LEFT JOIN TMP_SOLAR_AGG solar
      ON solar.MELO = meta.MELO
    LEFT JOIN TMP_BAT_AGG bat
      ON bat.MELO = meta.MELO;
    """)

    if build_views:
        # 6) Customer match View mit präziser MATCH-Logik
        stmts.append(f"""
        CREATE OR REPLACE VIEW {db}.{schema}.{DEFAULT_CUSTOMER_MATCH_VIEW} AS
        WITH netz_meta AS (
          SELECT
            MELO,
            MAX(IFF(LOKATION_MASTR_NUMMER IS NOT NULL AND TRIM(LOKATION_MASTR_NUMMER) <> '', 1, 0)) AS HAS_SEL_IN_MASTR,
            MAX(IFF(NETZANSCHLUSSPUNKT_MASTR_NUMMER IS NOT NULL AND TRIM(NETZANSCHLUSSPUNKT_MASTR_NUMMER) <> '', 1, 0)) AS HAS_SAN_IN_MASTR,
            1 AS MELO_PRESENT_IN_MASTR
          FROM {cur_netz}
          WHERE MELO IS NOT NULL AND TRIM(MELO) <> ''
          GROUP BY MELO
        )
        SELECT
          cust.METERINGCODE,
          cust."Salesforce Customer ID",
          cust."Salesforce Account ID",
          cust.GCID,
          cust.ABN_NR,
          cust.ABN_PLZ,
          cust.ABN_ORT,
          cust.ABN_STRASSE,
          cust.ABN_HAUSNR,

          s.MELO,
          s.SEL,
          s.SAN,
          s.SOLAR_UNITS,
          s.TOTAL_SOLAR_KWP,
          s.STORAGE_UNITS,
          s.TOTAL_STORAGE_KW,
          s.SOLAR_NAMES,
          s.STORAGE_NAMES,
          s.RAW_SOLAR_LIST,
          s.RAW_STORAGE_LIST,

          COALESCE(n.MELO_PRESENT_IN_MASTR, 0) AS MELO_PRESENT_IN_MASTR,
          COALESCE(n.HAS_SEL_IN_MASTR, 0)      AS HAS_SEL_IN_MASTR,
          COALESCE(n.HAS_SAN_IN_MASTR, 0)      AS HAS_SAN_IN_MASTR,

          CASE
            WHEN cust.METERINGCODE IS NULL THEN 'CUSTOMER_NO_MELO'
            WHEN COALESCE(n.MELO_PRESENT_IN_MASTR, 0) = 0 THEN 'NO_MATCH'
            WHEN COALESCE(n.HAS_SEL_IN_MASTR, 0) = 1
             AND COALESCE(n.HAS_SAN_IN_MASTR, 0) = 1
             AND COALESCE(s.SOLAR_UNITS, 0) + COALESCE(s.STORAGE_UNITS, 0) > 0 THEN 'MATCH'
            WHEN COALESCE(n.HAS_SEL_IN_MASTR, 0) = 1
             AND COALESCE(n.HAS_SAN_IN_MASTR, 0) = 1
             AND COALESCE(s.SOLAR_UNITS, 0) + COALESCE(s.STORAGE_UNITS, 0) = 0 THEN 'MISSING_UNITS'
            ELSE 'INCOMPLETE_MASTR_MAPPING'
          END AS MATCH_STATUS

        FROM {customer_table_fqn} cust
        LEFT JOIN {summary_fqn} s
          ON cust.METERINGCODE = s.MELO
        LEFT JOIN netz_meta n
          ON cust.METERINGCODE = n.MELO;
        """)

        # 7) Diagnose-View: MaStR SEL ohne MELO
        stmts.append(f"""
        CREATE OR REPLACE VIEW {db}.{schema}.{DEFAULT_DIAG_VIEW} AS
        SELECT DISTINCT
          LOKATION_MASTR_NUMMER AS SEL,
          NETZANSCHLUSSPUNKT_MASTR_NUMMER AS SAN
        FROM {cur_netz}
        WHERE MELO IS NULL OR TRIM(MELO) = '';
        """)

    return stmts


def run_summary_only(conn, cfg: SnowflakeCfg, export_date: str, customer_table_fqn: str, build_views: bool = True):
    stmts = build_summary_sql(
        db=cfg.database,
        schema=cfg.schema,
        summary_table=DEFAULT_SUMMARY_TABLE,
        customer_table_fqn=customer_table_fqn,
        export_date=export_date,
        build_views=build_views,
    )
    for s in stmts:
        run_sql(conn, s)
    logging.info("Summary built: %s.%s", cfg.schema, DEFAULT_SUMMARY_TABLE)
    if build_views:
        logging.info("Customer match view built: %s.%s", cfg.schema, DEFAULT_CUSTOMER_MATCH_VIEW)
        logging.info("Diagnostic view built: %s.%s", cfg.schema, DEFAULT_DIAG_VIEW)


# -----------------------------
# Snowflake config from env
# -----------------------------
def snowflake_cfg_from_env(args) -> 'SnowflakeCfg':
    def need(name: str) -> str:
        v = os.getenv(name)
        if not v:
            raise RuntimeError(f"Missing env var: {name}")
        return v

    return SnowflakeCfg(
        account=need("SNOWFLAKE_ACCOUNT"),
        user=need("SNOWFLAKE_USER"),
        role=need("SNOWFLAKE_ROLE"),
        warehouse=need("SNOWFLAKE_WAREHOUSE"),
        database=need("SNOWFLAKE_DATABASE"),
        schema=need("SNOWFLAKE_SCHEMA"),
        auth=args.auth,
        private_key_path=args.private_key_path,
        private_key_passphrase=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
    )


# -----------------------------
# CLI
# -----------------------------
def parse_args():
    default_base = Path(__file__).resolve().parent

    p = argparse.ArgumentParser()
    p.add_argument("--base-dir", default=str(default_base), help="Base directory. Default = folder of update.py")
    p.add_argument("--download-page-url", default=DEFAULT_DOWNLOAD_PAGE)
    p.add_argument("--download-url", default=None)

    p.add_argument("--env-file", default=".env", help="Path to .env file with SNOWFLAKE_* variables (relative to base-dir)")
    p.add_argument("--env-override", action="store_true")

    p.add_argument("--auth", choices=["externalbrowser", "jwt"], default="externalbrowser")
    p.add_argument("--private-key-path", default=None)

    p.add_argument("--log-level", default="INFO")
    p.add_argument("--no-progress", action="store_true")

    p.add_argument("--force-download", action="store_true")
    p.add_argument("--download-only", action="store_true")

    p.add_argument("--keep-old-zips", action="store_true")
    p.add_argument("--touch-unchanged", action="store_true")
    p.add_argument("--keep-stage-data", action="store_true")

    p.add_argument("--summary-only", action="store_true", help="Build only summary tables/views from existing CUR_*")
    p.add_argument("--skip-summary", action="store_true", help="Skip summary after load (for pipeline)")
    p.add_argument("--customer-table-fqn", default=DEFAULT_CUSTOMER_TABLE_FQN, help="FQN of CRM customer table for the view")

    p.add_argument("-n", "--max-files-per-type", dest="max_files_per_type", type=int, default=0)
    return p.parse_args()


# -----------------------------
# Main
# -----------------------------
def main():
    args = parse_args()

    base_dir = Path(args.base_dir).resolve()
    cache_dir = base_dir / "cache"
    run_dir = cache_dir / "run"

    cache_dir.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)

    progress = not args.no_progress
    prune_old_zips = not bool(args.keep_old_zips)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    # Determine export_date
    export_date = "unknown_date"
    meta_path = run_dir / "last_run_meta.json"
    if args.summary_only:
        # Prefer last run's export_date, otherwise leave unknown_date
        if meta_path.exists():
            try:
                prev = json.loads(meta_path.read_text(encoding="utf-8"))
                export_date = prev.get("export_date", "unknown_date")
            except Exception:
                pass
    else:
        # In full pipeline we try to derive export_date from URL
        dl_url = args.download_url or discover_latest_zip_url(args.download_page_url)
        export_date = extract_export_date_from_url(dl_url) or "unknown_date"

    log_path = run_dir / f"update_{export_date}.log"
    setup_logging(log_path, args.log_level, file_mode="a")

    run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + f"{random.randrange(16**6):06x}"
    fetch_ts = utc_now()

    logging.info("============================================================")
    logging.info("Run started. run_id=%s export_date=%s", run_id, export_date)
    logging.info("Base dir: %s", base_dir)
    logging.info("Cache ZIP dir: %s", cache_dir)
    logging.info("Run/log dir: %s", run_dir)
    logging.info("Log file: %s", log_path)

    # .env
    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        env_path = base_dir / env_path

    if env_path.exists():
        logging.info("Loading env file: %s (override=%s)", env_path, args.env_override)
        load_env_file(env_path, override=args.env_override)
    else:
        logging.info("No env file found at: %s (skipping)", env_path)

    required_env = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]
    missing = missing_required_env(required_env)
    if missing:
        logging.error("Missing required env vars: %s", ", ".join(missing))
        sys.exit(2)

    conn = None
    cfg: Optional[SnowflakeCfg] = None

    # Summary-only mode: skip download/extract/load
    if args.summary_only:
        try:
            cfg = snowflake_cfg_from_env(args)
            conn = connect_snowflake(cfg)
            conn.autocommit(True)

            who = run_sql(conn, "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            if who and who[0]:
                logging.info(
                    "Snowflake session. CURRENT_USER=%s | ROLE=%s | WH=%s | DB=%s | SCHEMA=%s",
                    who[0][0], who[0][1], who[0][2], who[0][3], who[0][4]
                )

            run_summary_only(conn, cfg, export_date, args.customer_table_fqn, build_views=True)
            logging.info("Summary-only completed.")
        finally:
            if conn is not None:
                conn.close()
            logging.info("Log file: %s", log_path)
        return

    # Normal pipeline
    try:
        download_url = args.download_url or discover_latest_zip_url(args.download_page_url)
        export_date2 = extract_export_date_from_url(download_url) or export_date
        if export_date2 != export_date:
            export_date = export_date2
            log_path = run_dir / f"update_{export_date}.log"
            setup_logging(log_path, args.log_level, file_mode="a")

        zip_path = ensure_zip_available(
            download_url=download_url,
            export_date=export_date,
            cache_dir=cache_dir,
            force_download=args.force_download,
            progress=progress,
            prune_old_zips=prune_old_zips,
        )
        zip_sha = sha256_file(zip_path)
        zip_size = zip_path.stat().st_size

        meta = {
            "run_id": run_id,
            "fetch_ts_utc": fetch_ts.isoformat(),
            "download_page_url": args.download_page_url,
            "download_url": download_url,
            "export_date": export_date,
            # keys expected by runs table
            "local_filename": str(zip_path),
            "size_bytes": int(zip_size),
            "sha256": str(zip_sha),
            # human-friendly duplicates
            "zip_path": str(zip_path),
            "zip_size_bytes": int(zip_size),
            "zip_sha256": str(zip_sha),
            "auth": args.auth,
            "download_only": bool(args.download_only),
            "prune_old_zips": bool(prune_old_zips),
            "max_files_per_type": int(args.max_files_per_type or 0),
            "log_file": str(log_path),
        }
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        logging.info("Wrote meta: %s", meta_path)

        if args.download_only:
            logging.info("Download-only mode enabled. Exiting.")
            return

        temp_extract_dir = run_dir / f"tmp_extract_{run_id}"

        # Extract to temp
        extracted_map = extract_selected_xml_to_temp(
            zip_path=zip_path,
            temp_dir=temp_extract_dir,
            progress=progress,
        )
        extracted_map = apply_max_files_per_dataset(extracted_map, int(args.max_files_per_type or 0))

        # Snowflake
        cfg = snowflake_cfg_from_env(args)
        conn = connect_snowflake(cfg)
        conn.autocommit(True)

        who = run_sql(conn, "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        if who and who[0]:
            logging.info(
                "Snowflake session. CURRENT_USER=%s | ROLE=%s | WH=%s | DB=%s | SCHEMA=%s",
                who[0][0], who[0][1], who[0][2], who[0][3], who[0][4]
            )

        ensure_tables(conn, cfg)
        log_run_start(conn, cfg, meta)

        # Load + Merge: NETZ
        netz_stage_cols = [
            "RUN_ID", "EXPORT_DATE", "SOURCE_FILE", "RECORD_HASH",
            "NETZANSCHLUSSPUNKT_MASTR_NUMMER", "LOKATION_MASTR_NUMMER", "MELO"
        ]
        load_xml_files_to_stage(
            conn=conn, cfg=cfg, run_id=run_id, export_date=export_date,
            dataset_name="NETZ",
            files=extracted_map["netzanschlusspunkte"],
            stg_table=STG_NETZ,
            key_col="NETZANSCHLUSSPUNKT_MASTR_NUMMER",
            record_mapper=lambda rec, p: map_netz(rec, p, run_id, export_date),
            wanted_tags=WANTED_TAGS_NETZ,
            progress=progress,
        )
        merge_stage_into_current(
            conn=conn, cfg=cfg, dataset_name="NETZ",
            stg_table=STG_NETZ, cur_table=CUR_NETZ,
            key_col="NETZANSCHLUSSPUNKT_MASTR_NUMMER",
            stage_cols=netz_stage_cols,
            touch_unchanged=bool(args.touch_unchanged),
        )
        if not args.keep_stage_data:
            run_sql(conn, f"TRUNCATE TABLE {fqn(cfg.database, cfg.schema, STG_NETZ)}")

        # Load + Merge: LOK
        lok_stage_cols = [
            "RUN_ID", "EXPORT_DATE", "SOURCE_FILE", "RECORD_HASH",
            "LOKATION_MASTR_NUMMER", "VERKNUEPFTE_EINHEITEN_MASTR_NUMMERN", "NETZANSCHLUSSPUNKTE_MASTR_NUMMERN"
        ]
        load_xml_files_to_stage(
            conn=conn, cfg=cfg, run_id=run_id, export_date=export_date,
            dataset_name="LOK",
            files=extracted_map["lokationen"],
            stg_table=STG_LOK,
            key_col="LOKATION_MASTR_NUMMER",
            record_mapper=lambda rec, p: map_lok(rec, p, run_id, export_date),
            wanted_tags=WANTED_TAGS_LOK,
            progress=progress,
        )
        merge_stage_into_current(
            conn=conn, cfg=cfg, dataset_name="LOK",
            stg_table=STG_LOK, cur_table=CUR_LOK,
            key_col="LOKATION_MASTR_NUMMER",
            stage_cols=lok_stage_cols,
            touch_unchanged=bool(args.touch_unchanged),
        )
        if not args.keep_stage_data:
            run_sql(conn, f"TRUNCATE TABLE {fqn(cfg.database, cfg.schema, STG_LOK)}")

        # Load + Merge: SOLAR
        solar_stage_cols = [
            "RUN_ID", "EXPORT_DATE", "SOURCE_FILE", "RECORD_HASH",
            "EINHEIT_MASTR_NUMMER", "LOKATION_MASTR_NUMMER", "POSTLEITZAHL", "ORT",
            "REGISTRIERUNGSDATUM", "INBETRIEBNAHMEDATUM", "BRUTTOLEISTUNG_RAW", "NETTONENNLEISTUNG_RAW",
            "ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW", "SPEICHERAMGLEICHENORT", "NAMESTROMERZEUGUNGSEINHEIT"
        ]
        load_xml_files_to_stage(
            conn=conn, cfg=cfg, run_id=run_id, export_date=export_date,
            dataset_name="SOLAR",
            files=extracted_map["einheiten_solar"],
            stg_table=STG_SOLAR,
            key_col="EINHEIT_MASTR_NUMMER",
            record_mapper=lambda rec, p: map_solar(rec, p, run_id, export_date),
            wanted_tags=WANTED_TAGS_SOLAR,
            progress=progress,
        )
        merge_stage_into_current(
            conn=conn, cfg=cfg, dataset_name="SOLAR",
            stg_table=STG_SOLAR, cur_table=CUR_SOLAR,
            key_col="EINHEIT_MASTR_NUMMER",
            stage_cols=solar_stage_cols,
            touch_unchanged=bool(args.touch_unchanged),
        )
        if not args.keep_stage_data:
            run_sql(conn, f"TRUNCATE TABLE {fqn(cfg.database, cfg.schema, STG_SOLAR)}")

        # Load + Merge: BAT
        bat_stage_cols = [
            "RUN_ID", "EXPORT_DATE", "SOURCE_FILE", "RECORD_HASH",
            "EINHEIT_MASTR_NUMMER", "LOKATION_MASTR_NUMMER", "POSTLEITZAHL", "ORT",
            "REGISTRIERUNGSDATUM", "INBETRIEBNAHMEDATUM", "BRUTTOLEISTUNG_RAW", "NETTONENNLEISTUNG_RAW",
            "ZUGEORDNETEWIRKLEISTUNGWECHSELRICHTER_RAW", "EEGMASTRNUMMER", "NAMESTROMERZEUGUNGSEINHEIT"
        ]
        load_xml_files_to_stage(
            conn=conn, cfg=cfg, run_id=run_id, export_date=export_date,
            dataset_name="BAT",
            files=extracted_map["einheiten_stromspeicher"],
            stg_table=STG_BAT,
            key_col="EINHEIT_MASTR_NUMMER",
            record_mapper=lambda rec, p: map_battery(rec, p, run_id, export_date),
            wanted_tags=WANTED_TAGS_BAT,
            progress=progress,
        )
        merge_stage_into_current(
            conn=conn, cfg=cfg, dataset_name="BAT",
            stg_table=STG_BAT, cur_table=CUR_BAT,
            key_col="EINHEIT_MASTR_NUMMER",
            stage_cols=bat_stage_cols,
            touch_unchanged=bool(args.touch_unchanged),
        )
        if not args.keep_stage_data:
            run_sql(conn, f"TRUNCATE TABLE {fqn(cfg.database, cfg.schema, STG_BAT)}")

        # Summary unless skipped
        if not args.skip_summary:
            run_summary_only(conn, cfg, export_date, args.customer_table_fqn, build_views=True)
        else:
            logging.info("Skipping summary as requested (--skip-summary).")

        log_run_end(conn, cfg, run_id, "SUCCESS", None)
        logging.info("Run SUCCESS. run_id=%s export_date=%s", run_id, export_date)

        # Cleanup temp extraction dir on success
        try:
            rmtree_with_retries(temp_extract_dir, retries=6, sleep_s=2.0)
            logging.info("Deleted TEMP extraction dir: %s", temp_extract_dir)
        except Exception as e:
            logging.warning("Could not delete TEMP extraction dir %s: %s", temp_extract_dir, e)

    except Exception as e:
        logging.exception("Run FAILED: %s", e)
        if conn is not None and cfg is not None:
            try:
                log_run_end(conn, cfg, run_id, "FAILED", str(e)[:8000])
            except Exception:
                pass
        raise
    finally:
        if conn is not None:
            conn.close()
        logging.info("Log file: %s", log_path)


if __name__ == "__main__":
    main()
