# ============================================================
# Stock Market ETL Pipeline — Phase 1: Data Extractor
# Project : Automated Financial Data Pipeline
# Author  : Shiva Kumar Devatha
# Tools   : Python (yfinance, Pandas)
# ============================================================
# SETUP — run once in terminal before this script:
#   pip install yfinance pandas
# ============================================================

import yfinance as yf
import pandas as pd
import os
import logging
from datetime import datetime, timedelta

# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "raw")
LOG_DIR    = os.path.join(BASE_DIR, "logs")

# Date range configuration
LOOKBACK_DAYS = 365

# Stocks to track — BFSI focused (US + India)
STOCKS = {
    "JPM": "JP Morgan Chase",
    "BAC": "Bank of America",
    "GS": "Goldman Sachs",
    "WFC": "Wells Fargo",
    "HDFCBANK.NS": "HDFC Bank",
    "ICICIBANK.NS": "ICICI Bank",
    "SBIN.NS": "State Bank of India"
}

END_DATE = datetime.today().strftime("%Y-%m-%d")
START_DATE = (
    datetime.today() - timedelta(days=LOOKBACK_DAYS)
).strftime("%Y-%m-%d")

# ============================================================
# 2. SETUP — FOLDERS & LOGGING
# ============================================================

# Create output and log directories if they don't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR,    exist_ok=True)

# Configure logging — writes to both console and log file
log_filename = os.path.join(
    LOG_DIR,
    f"extractor_{datetime.today().strftime('%Y%m%d_%H%M%S')}.log"
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
# 3. EXTRACTOR FUNCTION
# ============================================================

def extract_stock(ticker, company_name, start, end):
    """
    Pull daily OHLCV data for a single stock from Yahoo Finance.
    Returns a cleaned DataFrame or None if extraction fails.

    Columns extracted:
        Open, High, Low, Close, Volume, Dividends, Stock Splits
    """
    try:
        logger.info(f"Extracting: {ticker} ({company_name})")

        stock = yf.Ticker(ticker)

        df = stock.history(
            start=start,
            end=end,
            interval="1d",       # daily frequency
            auto_adjust=True,    # adjust for splits/dividends
            actions=True         # include dividend/split columns
        )

        if df.empty:
            logger.warning(f"No data returned for {ticker} — skipping.")
            return None

        # Reset index so Date becomes a column
        df.reset_index(inplace=True)

        # Add identifier columns
        df.insert(0, "ticker",       ticker)
        df.insert(1, "company_name", company_name)

        # Standardise column names
        df.columns = [
            col.lower().replace(" ", "_")
            for col in df.columns
        ]

        # Keep only relevant columns
        keep_cols = [
            "ticker",
            "company_name",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dividends",
            "stock splits"
        ]

        df = df[[c for c in keep_cols if c in df.columns]]

        # Rename stock splits column
        if "stock splits" in df.columns:
            df.rename(
                columns={"stock splits": "stock_splits"},
                inplace=True
            )

        # Ensure date is date only (no time component)
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # Round numeric columns to 4 decimal places
        num_cols = ["open", "high", "low", "close"]
        df[num_cols] = df[num_cols].round(4)

        # Volume as integer
        df["volume"] = df["volume"].fillna(0).astype(int)

        # Check invalid close prices
        if df["close"].le(0).any():
         logger.warning(f"{ticker}: Invalid close price detected")

        # Check total null values
        logger.info(
            f"{ticker}: Null Count = {df.isnull().sum().sum()}"
        )

        logger.info(
            f" [SUCCESS] {ticker}: {len(df)} rows | "
            f"{df['date'].min()} to {df['date'].max()}"
        )
        df["extracted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df

    except Exception as e:
        logger.error(f" [FAILED] Failed to extract {ticker}: {e}")
        return None


# ============================================================
# 4. MAIN EXTRACTION LOOP
# ============================================================

def run_extraction():
    logger.info("=" * 60)
    logger.info("STOCK MARKET ETL PIPELINE — PHASE 1: EXTRACTION")
    logger.info("=" * 60)
    logger.info(f"Date range  : {START_DATE} to {END_DATE}")
    logger.info(f"Stocks      : {len(STOCKS)}")
    logger.info(f"Output dir  : {OUTPUT_DIR}")
    logger.info("=" * 60)

    all_data    = []
    success     = []
    failed      = []

    for ticker, company in STOCKS.items():
        df = extract_stock(ticker, company, START_DATE, END_DATE)

        if df is not None:
            all_data.append(df)
            success.append(ticker)

            # Save individual stock file
            individual_path = os.path.join(
                OUTPUT_DIR,
                f"{ticker.replace('.', '_')}_raw.csv"
            )
            df.to_csv(individual_path, index=False)
            logger.info(f" [SAVED] {individual_path}")

        else:
            failed.append(ticker)

    # ── Combine all stocks into one master file ───────────────
    if all_data:
        master_df = pd.concat(all_data, ignore_index=True)

        # Sort by ticker and date
        master_df.sort_values(
            ["ticker", "date"],
            inplace=True
        )
        # ETL extraction timestamp
        master_df["extracted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        master_path = os.path.join(OUTPUT_DIR, "all_stocks_raw.csv")
        master_df.to_csv(master_path, index=False)

        logger.info("")
        logger.info("=" * 60)
        logger.info("EXTRACTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Successful  : {len(success)} stocks — {success}")
        logger.info(f"Failed      : {len(failed)} stocks — {failed}")
        logger.info(f"Total rows  : {len(master_df):,}")
        logger.info(f"Columns     : {list(master_df.columns)}")
        logger.info(f"Master file : {master_path}")
        logger.info(f"Log file    : {log_filename}")
        logger.info("=" * 60)
        logger.info("Phase 1 complete. Ready for Phase 2 — Transformer.")
        logger.info("=" * 60)

        # Quick preview
        print("\n--- SAMPLE OUTPUT (first 5 rows) ---")
        print(master_df.head().to_string(index=False))

        # Quick stats per stock
        print("\n--- RECORDS PER STOCK ---")
        print(
            master_df.groupby("ticker")["date"]
            .count()
            .rename("trading_days")
            .to_string()
        )

        return master_df

    else:
        logger.error("No data extracted. Check internet connection or ticker symbols.")
        return None


# ============================================================
# 5. RUN
# ============================================================

if __name__ == "__main__":
    df = run_extraction()

    if df is not None:
        print("\n[SUCCESS] Extraction complete.")
        print(f"[INFO]    Next step -> Run phase2_transformer.py")
    else:
        print("\n[ERROR] Extraction failed. Check logs for details.")

        
