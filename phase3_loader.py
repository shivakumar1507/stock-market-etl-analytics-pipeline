# ============================================================
# Stock Market ETL Pipeline — Phase 3: Loader
# Project : Automated Financial Data Pipeline
# Author  : Shiva Kumar Devatha
# Tools   : Python (Pandas, SQLite3)
# Input   : data/processed/all_stocks_transformed.csv  (Phase 2 output)
# Output  : data/database/stock_market.db
# ============================================================
# SETUP — no extra installs needed
#   sqlite3 and json are part of Python standard library
# ============================================================

import sqlite3
import pandas as pd
import os
import json
import logging
from datetime import datetime

# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE   = os.path.join(BASE_DIR, "data", "processed", "all_stocks_transformed.csv")
DB_DIR       = os.path.join(BASE_DIR, "data", "database")
DB_PATH      = os.path.join(DB_DIR,  "stock_market.db")
LOG_DIR      = os.path.join(BASE_DIR, "logs")

# Stock metadata — exchange, country, sector per ticker
# Used to populate the stock_metadata dimension table
STOCK_META = {
    "JPM":          {"exchange": "NYSE",  "country": "USA",   "sector": "Banking"},
    "BAC":          {"exchange": "NYSE",  "country": "USA",   "sector": "Banking"},
    "GS":           {"exchange": "NYSE",  "country": "USA",   "sector": "Investment Banking"},
    "WFC":          {"exchange": "NYSE",  "country": "USA",   "sector": "Banking"},
    "HDFCBANK.NS":  {"exchange": "NSE",   "country": "India", "sector": "Banking"},
    "ICICIBANK.NS": {"exchange": "NSE",   "country": "India", "sector": "Banking"},
    "SBIN.NS":      {"exchange": "NSE",   "country": "India", "sector": "Banking"},
}

# ============================================================
# 2. SETUP — FOLDERS & LOGGING
# ============================================================

os.makedirs(DB_DIR,  exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = os.path.join(
    LOG_DIR,
    f"loader_{datetime.today().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ============================================================
# 3. DDL — TABLE DEFINITIONS
# ============================================================

DDL_STOCK_PRICES = """
CREATE TABLE IF NOT EXISTS stock_prices (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                TEXT    NOT NULL,
    company_name          TEXT    NOT NULL,
    date                  DATE    NOT NULL,
    open                  REAL,
    high                  REAL,
    low                   REAL,
    close                 REAL    NOT NULL,
    volume                INTEGER,
    dividends             REAL,
    stock_splits          REAL,
    daily_return_pct      REAL,
    ma_7                  REAL,
    ma_21                 REAL,
    ma_50                 REAL,
    volatility_21         REAL,
    rsi_14                REAL,
    price_range           REAL,
    price_range_pct       REAL,
    volume_change_pct     REAL,
    cumulative_return_pct REAL,
    above_ma50            INTEGER,
    rsi_zone              TEXT,
    signal                TEXT,
    transformed_at        TEXT,
    loaded_at             TEXT    NOT NULL,
    UNIQUE(ticker, date)
);
"""

DDL_STOCK_METADATA = """
CREATE TABLE IF NOT EXISTS stock_metadata (
    ticker         TEXT    PRIMARY KEY,
    company_name   TEXT,
    exchange       TEXT,
    country        TEXT,
    sector         TEXT,
    first_date     DATE,
    last_date      DATE,
    total_records  INTEGER,
    avg_close      REAL,
    avg_volume     REAL,
    loaded_at      TEXT
);
"""

DDL_ETL_AUDIT_LOG = """
CREATE TABLE IF NOT EXISTS etl_audit_log (
    run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    phase         TEXT    NOT NULL,
    run_timestamp TEXT    NOT NULL,
    rows_loaded   INTEGER,
    tickers       TEXT,
    status        TEXT    NOT NULL,
    notes         TEXT
);
"""

# ============================================================
# 4. INDEX DEFINITIONS
# ============================================================

INDEXES = [
    # Most queries filter by ticker — single most important index
    "CREATE INDEX IF NOT EXISTS idx_ticker          ON stock_prices(ticker);",

    # Time-series queries filter by date
    "CREATE INDEX IF NOT EXISTS idx_date            ON stock_prices(date);",

    # Most common query pattern — ticker + date range together
    "CREATE INDEX IF NOT EXISTS idx_ticker_date     ON stock_prices(ticker, date);",

    # Signal-based filtering for dashboard queries
    "CREATE INDEX IF NOT EXISTS idx_signal          ON stock_prices(signal);",

    # RSI zone filtering
    "CREATE INDEX IF NOT EXISTS idx_rsi_zone        ON stock_prices(rsi_zone);",

    # Country-level filtering on metadata
    "CREATE INDEX IF NOT EXISTS idx_country         ON stock_metadata(country);",
]

# ============================================================
# 5. VALIDATION FUNCTION
# ============================================================

def validate_input(df):
    """
    Validate the transformed CSV before loading into the database.
    Raises ValueError on critical failures.
    """
    logger.info("Running pre-load validation ...")

    # Check required columns
    required = [
        "ticker", "company_name", "date", "open", "high",
        "low", "close", "volume", "daily_return_pct",
        "ma_7", "ma_21", "ma_50", "rsi_14", "signal"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Check not empty
    if df.empty:
        raise ValueError("Transformed file is empty — nothing to load.")

    # Check for duplicate ticker+date combinations
    dupes = df.duplicated(subset=["ticker", "date"]).sum()
    if dupes > 0:
        logger.warning(f"Found {dupes} duplicate ticker+date rows — will be handled by UNIQUE constraint.")

    # Check close prices
    bad_close = (df["close"] <= 0).sum()
    if bad_close > 0:
        logger.warning(f"{bad_close} rows have zero/negative close prices.")

    # Log null summary for key columns
    key_cols  = ["close", "daily_return_pct", "ma_7", "rsi_14", "signal"]
    null_info = df[key_cols].isnull().sum()
    logger.info(f"Null counts (key columns):\n{null_info.to_string()}")

    logger.info(
        f"Validation passed — {len(df):,} rows, "
        f"{df['ticker'].nunique()} tickers, "
        f"date range: {df['date'].min()} to {df['date'].max()}"
    )


# ============================================================
# 6. DATABASE FUNCTIONS
# ============================================================

def create_tables(cursor):
    """Create all tables if they don't already exist."""
    logger.info("Creating tables (if not exists) ...")
    cursor.execute(DDL_STOCK_PRICES)
    cursor.execute(DDL_STOCK_METADATA)
    cursor.execute(DDL_ETL_AUDIT_LOG)
    logger.info("Tables ready: stock_prices | stock_metadata | etl_audit_log")


def create_indexes(cursor):
    """Create indexes for query performance."""
    logger.info("Creating indexes ...")
    for ddl in INDEXES:
        cursor.execute(ddl)
    logger.info(f"{len(INDEXES)} indexes created/verified.")


def truncate_tables(cursor):
    """
    Clear existing data before reload.
    Truncate-and-reload strategy — simple and safe for batch pipelines.
    Preserves audit log (intentionally not truncated).
    """
    logger.info("Truncating existing data (truncate-and-reload strategy) ...")
    cursor.execute("DELETE FROM stock_prices;")
    cursor.execute("DELETE FROM stock_metadata;")
    logger.info("stock_prices and stock_metadata cleared.")


def load_stock_prices(df, conn, loaded_at):
    """
    Bulk load the main fact table using pandas to_sql.
    Adds loaded_at timestamp for ETL audit trail.
    """
    df = df.copy()
    df["loaded_at"] = loaded_at
    
    # Drop any columns not in the DDL
    # (e.g. extracted_at from Phase 1 that survived into the CSV)

    allowed_cols = [
        "ticker", "company_name", "date", "open", "high", "low", "close",
        "volume", "dividends", "stock_splits", "daily_return_pct",
        "ma_7", "ma_21", "ma_50", "volatility_21", "rsi_14",
        "price_range", "price_range_pct", "volume_change_pct",
        "cumulative_return_pct", "above_ma50", "rsi_zone",
        "signal", "transformed_at", "loaded_at"
    ]
    extra_cols = [c for c in df.columns if c not in allowed_cols]

    if extra_cols:
        logger.warning(f"Dropping unexpected columns not in DDL: {extra_cols}")
        df.drop(columns=extra_cols, inplace=True)

    # Drop the auto-increment id if somehow present
    if "id" in df.columns:
        df.drop(columns=["id"], inplace=True)

    # Convert date to string for SQLite compatibility
    df["date"] = df["date"].astype(str)

    # Convert above_ma50 to int safely
    df["above_ma50"] = pd.to_numeric(df["above_ma50"], errors="coerce").astype("Int64")

    rows_before = len(df)

    df.to_sql(
        name      = "stock_prices",
        con       = conn,
        if_exists = "append",   # table already created — just append
        index     = False,
        chunksize = 500,        # batch insert for memory efficiency
        method    = "multi"     # faster multi-row INSERT
    )

    logger.info(f"Loaded {rows_before:,} rows into stock_prices.")
    return rows_before


def build_and_load_metadata(df, cursor, conn, loaded_at):
    """
    Derive stock_metadata dimension table from loaded fact data.
    Joins with STOCK_META config for exchange/country/sector.
    """
    logger.info("Building stock_metadata dimension table ...")

    summary = df.groupby(["ticker", "company_name"]).agg(
        first_date    = ("date", "min"),
        last_date     = ("date", "max"),
        total_records = ("close", "count"),
        avg_close     = ("close", "mean"),
        avg_volume    = ("volume", "mean"),
    ).reset_index()

    summary["avg_close"]  = summary["avg_close"].round(4)
    summary["avg_volume"] = summary["avg_volume"].round(0).astype(int)
    summary["first_date"] = summary["first_date"].astype(str)
    summary["last_date"]  = summary["last_date"].astype(str)
    summary["loaded_at"]  = loaded_at

    # Enrich with exchange / country / sector from config
    summary["exchange"] = summary["ticker"].map(lambda t: STOCK_META.get(t, {}).get("exchange", "Unknown"))
    summary["country"]  = summary["ticker"].map(lambda t: STOCK_META.get(t, {}).get("country",  "Unknown"))
    summary["sector"]   = summary["ticker"].map(lambda t: STOCK_META.get(t, {}).get("sector",   "Unknown"))

    # Reorder columns to match DDL
    summary = summary[[
        "ticker", "company_name", "exchange", "country", "sector",
        "first_date", "last_date", "total_records", "avg_close", "avg_volume", "loaded_at"
    ]]

    summary.to_sql(
        name      = "stock_metadata",
        con       = conn,
        if_exists = "append",
        index     = False
    )

    logger.info(f"Loaded {len(summary)} rows into stock_metadata.")
    return summary


def write_audit_log(cursor, rows_loaded, tickers, status, notes=""):
    """Write a record to etl_audit_log for every pipeline run."""
    cursor.execute("""
        INSERT INTO etl_audit_log (phase, run_timestamp, rows_loaded, tickers, status, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        "phase3_loader",
        datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
        rows_loaded,
        json.dumps(tickers),
        status,
        notes
    ))
    logger.info(f"Audit log written — status: {status} | rows: {rows_loaded:,}")


def verify_load(cursor):
    """
    Post-load verification — row counts, date ranges, signal distribution.
    Proves data landed correctly.
    """
    logger.info("Running post-load verification ...")

    # Total row count
    cursor.execute("SELECT COUNT(*) FROM stock_prices;")
    total = cursor.fetchone()[0]
    logger.info(f"stock_prices total rows : {total:,}")

    # Per-ticker row counts
    cursor.execute("""
        SELECT ticker, COUNT(*) AS rows,
               MIN(date) AS first_date,
               MAX(date) AS last_date
        FROM stock_prices
        GROUP BY ticker
        ORDER BY ticker;
    """)
    rows = cursor.fetchall()
    logger.info("Per-ticker verification:")
    for row in rows:
        logger.info(f"  {row[0]:<15} {row[1]} rows  |  {row[2]} : {row[3]}")

    # Signal distribution
    cursor.execute("""
        SELECT signal, COUNT(*) AS count
        FROM stock_prices
        GROUP BY signal
        ORDER BY count DESC;
    """)
    signals = cursor.fetchall()
    logger.info("Signal distribution:")
    for s in signals:
        logger.info(f"  {s[0]:<25} {s[1]:,} rows")

    # RSI zone distribution
    cursor.execute("""
        SELECT rsi_zone, COUNT(*) AS count
        FROM stock_prices
        WHERE rsi_zone IS NOT NULL
        GROUP BY rsi_zone
        ORDER BY count DESC;
    """)
    zones = cursor.fetchall()
    logger.info("RSI zone distribution:")
    for z in zones:
        logger.info(f"  {z[0]:<20} {z[1]:,} rows")

    # Metadata table check
    cursor.execute("SELECT ticker, exchange, country, sector, total_records FROM stock_metadata;")
    meta_rows = cursor.fetchall()
    logger.info("stock_metadata contents:")
    for m in meta_rows:
        logger.info(f"  {m[0]:<15} {m[1]:<6} {m[2]:<8} {m[3]:<25} {m[4]} records")

    return total


# ============================================================
# 7. MAIN LOADER FUNCTION
# ============================================================

def run_loader():
    logger.info("=" * 60)
    logger.info("STOCK MARKET ETL PIPELINE — PHASE 3: LOADER")
    logger.info("=" * 60)
    logger.info(f"Input file  : {INPUT_FILE}")
    logger.info(f"Database    : {DB_PATH}")
    logger.info("=" * 60)

    loaded_at = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    status    = "FAILED"
    rows_loaded = 0
    tickers   = []

    # ── STEP 1: Load transformed CSV ─────────────────────────
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file not found: {INPUT_FILE}")
        logger.error("Make sure Phase 2 (phase2_transformer.py) has been run first.")
        return None

    df = pd.read_csv(INPUT_FILE, parse_dates=["date"])
    logger.info(f"Loaded {len(df):,} rows from Phase 2 output.")

    # ── STEP 2: Validate ──────────────────────────────────────
    try:
        validate_input(df)
    except ValueError as e:
        logger.error(f"Validation failed: {e}")
        return None

    tickers = df["ticker"].unique().tolist()

    # ── STEP 3: Connect to SQLite ─────────────────────────────
    logger.info(f"Connecting to SQLite database: {DB_PATH}")
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Enable WAL mode for better write performance
    cursor.execute("PRAGMA journal_mode=WAL;")
    logger.info("SQLite connection established (WAL mode enabled).")

    try:
        # ── STEP 4: Create tables ─────────────────────────────
        create_tables(cursor)

        # ── STEP 5: Truncate existing data ────────────────────
        truncate_tables(cursor)

        # ── STEP 6: Load stock_prices (main fact table) ───────
        rows_loaded = load_stock_prices(df, conn, loaded_at)

        # ── STEP 7: Build & load stock_metadata ───────────────
        build_and_load_metadata(df, cursor, conn, loaded_at)

        # ── STEP 8: Create indexes ────────────────────────────
        create_indexes(cursor)

        # ── STEP 9: Commit ────────────────────────────────────
        conn.commit()
        logger.info("Transaction committed successfully.")

        # ── STEP 10: Post-load verification ───────────────────
        total_verified = verify_load(cursor)

        logger.info(f"Rows inserted : {rows_loaded:,}")
        logger.info(f"Rows verified : {total_verified:,}")

        # ── STEP 11: Write audit log ──────────────────────────
        status = "SUCCESS"
        write_audit_log(
            cursor,
            rows_loaded = total_verified,
            tickers     = tickers,
            status      = status,
            notes       = f"Full reload. {len(tickers)} tickers. Indexes refreshed."
        )
        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed — transaction rolled back. Error: {e}")
        write_audit_log(cursor, 0, tickers, "FAILED", str(e))
        conn.commit()
        raise

    finally:
        conn.close()
        logger.info("Database connection closed.")

    # ── Final Summary ─────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("LOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Status          : {status}")
    logger.info(f"Rows loaded     : {rows_loaded:,}")
    logger.info(f"Tickers         : {tickers}")
    logger.info(f"Tables loaded   : stock_prices | stock_metadata")
    logger.info(f"Indexes created : {len(INDEXES)}")
    logger.info(f"Database path   : {DB_PATH}")
    logger.info(f"Log file        : {log_filename}")
    logger.info("=" * 60)
    logger.info("Phase 3 complete. Ready for Phase 4 — SQL Analysis.")
    logger.info("=" * 60)

    return DB_PATH


# ============================================================
# 8. RUN
# ============================================================

if __name__ == "__main__":
    result = run_loader()

    if result:
        print("\n[SUCCESS] Load complete.")
        print(f"[INFO]    Database : {result}")
        print(f"[INFO]    Open with: DB Browser for SQLite (https://sqlitebrowser.org)")
        print(f"[INFO]    Next step : Run phase4_analyzer.py")
    else:
        print("\n[ERROR] Load failed. Check logs for details.")
