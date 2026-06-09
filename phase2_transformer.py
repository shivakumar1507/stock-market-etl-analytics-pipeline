# ============================================================
# Stock Market ETL Pipeline — Phase 2: Transformer
# Project : Automated Financial Data Pipeline
# Author  : Shiva Kumar Devatha
# Tools   : Python (Pandas, NumPy)
# Input   : data/raw/all_stocks_raw.csv       (Phase 1 output)
# Output  : data/processed/all_stocks_transformed.csv
# ============================================================
# SETUP — run once in terminal before this script:
#   pip install pandas numpy
# ============================================================

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

# ============================================================
# 1. CONFIGURATION
# ============================================================

# Use relative paths — works on any machine (fixes Phase 1 issue)
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE   = os.path.join(BASE_DIR, "data", "raw",       "all_stocks_raw.csv")
OUTPUT_DIR   = os.path.join(BASE_DIR, "data", "processed")
OUTPUT_FILE  = os.path.join(OUTPUT_DIR, "all_stocks_transformed.csv")
LOG_DIR      = os.path.join(BASE_DIR, "logs")

# Rolling window sizes
MA_SHORT     = 7     # short-term moving average
MA_MID       = 21    # mid-term moving average
MA_LONG      = 50    # long-term moving average
RSI_PERIOD   = 14    # standard RSI period
VOLAT_WINDOW = 21    # volatility lookback window

# Minimum rows a stock must have after transform (drop if below)
MIN_ROWS_THRESHOLD = 50

# ============================================================
# 2. SETUP — FOLDERS & LOGGING
# ============================================================

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR,    exist_ok=True)

log_filename = os.path.join(
    LOG_DIR,
    f"transformer_{datetime.today().strftime('%Y%m%d_%H%M%S')}.log"
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
# 3. VALIDATION FUNCTION
# ============================================================

def validate_raw_data(df):
    """
    Run basic sanity checks on the raw data before transforming.
    Raises ValueError if critical checks fail.
    """
    logger.info("Running data validation ...")

    # Check required columns exist
    required_cols = ["ticker", "company_name", "date", "open", "high", "low", "close", "volume"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Check for completely empty dataframe
    if df.empty:
        raise ValueError("Input file is empty — nothing to transform.")

    # Check close prices are positive
    invalid_close = df[df["close"] <= 0]
    if not invalid_close.empty:
        logger.warning(
            f"Found {len(invalid_close)} rows with zero/negative close price — "
            f"tickers affected: {invalid_close['ticker'].unique().tolist()}"
        )

    # Check null percentages per column
    null_pct = (df.isnull().mean() * 100).round(2)
    high_null_cols = null_pct[null_pct > 5]
    if not high_null_cols.empty:
        logger.warning(f"High null % detected:\n{high_null_cols.to_string()}")
    else:
        logger.info("Null check passed — all columns below 5% null threshold.")

    # Log stock-wise row counts
    stock_counts = df.groupby("ticker")["date"].count().rename("rows")
    logger.info(f"Row counts per ticker:\n{stock_counts.to_string()}")

    logger.info(f"Validation complete — {len(df):,} rows, {df['ticker'].nunique()} tickers loaded.")


# ============================================================
# 4. FEATURE ENGINEERING FUNCTIONS
# ============================================================

def compute_rsi(series, period=RSI_PERIOD):
    """
    Compute RSI (Relative Strength Index) for a price series.

    RSI > 70  :  Overbought (potential sell signal)
    RSI < 30  :  Oversold   (potential buy signal)
    """
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs  = avg_gain / avg_loss.replace(0, np.nan)   # avoid division by zero
    rsi = 100 - (100 / (1 + rs))
    return rsi.round(2)


def compute_signal(row):
    """
    Determine Buy / Sell / Hold signal based on MA crossover.

    Buy  : short MA crosses above mid MA  (Golden Cross)
    Sell : short MA crosses below mid MA  (Death Cross)
    Hold : no clear crossover signal
    """
    if pd.isna(row[f"ma_{MA_SHORT}"]) or pd.isna(row[f"ma_{MA_MID}"]):
        return "Insufficient Data"
    elif row[f"ma_{MA_SHORT}"] > row[f"ma_{MA_MID}"]:
        return "Buy"
    elif row[f"ma_{MA_SHORT}"] < row[f"ma_{MA_MID}"]:
        return "Sell"
    else:
        return "Hold"


def transform_stock(df):
    """
    Apply all feature engineering to a single stock's DataFrame.
    Input  : raw OHLCV DataFrame for ONE ticker (sorted by date)
    Output : enriched DataFrame with all engineered features
    """
    ticker = df["ticker"].iloc[0]

    df = df.copy().sort_values("date").reset_index(drop=True)

    # ---- 1. Daily Return (%) --------------------------------------------------------------------
    # How much % the stock gained or lost vs previous day
    df["daily_return_pct"] = (df["close"].pct_change() * 100).round(4)

    # ---- 2. Moving Averages ------------------------------------------------------------------------
    # Trend indicators — short, mid, and long term
    df[f"ma_{MA_SHORT}"] = df["close"].rolling(window=MA_SHORT, min_periods=MA_SHORT).mean().round(4)
    df[f"ma_{MA_MID}"]   = df["close"].rolling(window=MA_MID,   min_periods=MA_MID).mean().round(4)
    df[f"ma_{MA_LONG}"]  = df["close"].rolling(window=MA_LONG,  min_periods=MA_LONG).mean().round(4)

    # ---- 3. Volatility (Rolling Std of Daily Returns) --------------------
    # Higher value = higher risk. Core BFSI risk metric.
    df["volatility_21"] = (
        df["daily_return_pct"]
        .rolling(window=VOLAT_WINDOW, min_periods=VOLAT_WINDOW)
        .std()
        .round(4)
    )

    # ---- 4. RSI — Relative Strength Index ----------------------------------------─
    # Momentum indicator: >70 overbought, <30 oversold
    df["rsi_14"] = compute_rsi(df["close"])

    # ---- 5. Price Range & Range % ------------------------------------------------------------
    # Intraday high-low spread — proxy for intraday volatility
    df["price_range"]     = (df["high"] - df["low"]).round(4)
    df["price_range_pct"] = ((df["price_range"] / df["close"]) * 100).round(4)

    # ---- 6. Volume Change (%) --------------------------------------------------------------------
    # Sudden spikes often precede price movements
    df["volume_change_pct"] = (
    df["volume"].pct_change() * 100
    ).round(2)

    df["volume_change_pct"] = df["volume_change_pct"].replace(
    [np.inf, -np.inf],
    np.nan
    )

    # ---- 7. Cumulative Return (%) ------------------------------------------------------------
    # Total % return since first trading day in the dataset
    first_close = df["close"].iloc[0]
    df["cumulative_return_pct"] = (((df["close"] / first_close) - 1) * 100).round(4)

    # ---- 8. Above / Below Long MA Flag ------------------------------------------------
    # Price position relative to 50-day MA
    df["above_ma50"] = np.where(
        df[f"ma_{MA_LONG}"].isna(), None,
        np.where(df["close"] > df[f"ma_{MA_LONG}"], 1, 0)
    )

    # ---- 9. RSI Zone Label ------------------------------------------------------------------------─
    # Human-readable RSI interpretation
    df["rsi_zone"] = pd.cut(
        df["rsi_14"],
        bins=[0, 30, 50, 70, 100],
        labels=["Oversold", "Neutral-Low", "Neutral-High", "Overbought"],
        right=True
    ).astype(str).replace("nan", None)

    # ---- 10. Buy / Sell / Hold Signal ----------------------------------------------------
    # Based on MA7 vs MA21 crossover (Golden Cross / Death Cross)
    df["signal"] = df.apply(compute_signal, axis=1)

    # ---- 11. Extraction Timestamp ------------------------------------------------------------
    df["transformed_at"] = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    return df


# ============================================================
# 5. MAIN TRANSFORM FUNCTION
# ============================================================

def run_transformation():
    logger.info("=" * 60)
    logger.info("STOCK MARKET ETL PIPELINE — PHASE 2: TRANSFORMATION")
    logger.info("=" * 60)
    logger.info(f"Input file  : {INPUT_FILE}")
    logger.info(f"Output dir  : {OUTPUT_DIR}")
    logger.info("=" * 60)

    # ---- STEP 1: Load raw data ----------------------------------------------------------------─
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file not found: {INPUT_FILE}")
        logger.error("Make sure Phase 1 (phase1_extractor.py) has been run first.")
        return None

    raw_df = pd.read_csv(INPUT_FILE, parse_dates=["date"])
    rows_before = len(raw_df)
    logger.info(f"Loaded {rows_before:,} rows from Phase 1 output.")

    # ---- STEP 2: Validate ----------------------------------------------------------------------------
    try:
        validate_raw_data(raw_df)
    except ValueError as e:
        logger.error(f"Validation failed: {e}")
        return None

    # ---- STEP 3: Transform per ticker ------------------------------------------------─
    # IMPORTANT: groupby ensures rolling calculations never
    # bleed across different stocks (common beginner mistake)
    logger.info("Applying feature engineering per ticker ...")

    # include_groups=False required in pandas 2.2+ to avoid KeyError on groupby column
    transformed_list = []

    for ticker, group in raw_df.groupby("ticker"):
        group = group.copy()  # avoid SettingWithCopyWarning
        group["ticker"] = ticker  # ensure ticker column is present in group

        transformed_list.append(transform_stock(group))
    transformed_df = pd.concat(transformed_list, ignore_index=True)

    rows_after = len(transformed_df)
    logger.info(f"Transform complete — {rows_after:,} rows retained (no row loss expected).")    


    # ---- STEP 4: Drop NaN warm-up rows ------------------------------------------------
    # Rolling windows produce NaN for first N rows — drop them
    # Keep rows only where at minimum daily_return is available
    pre_drop = len(transformed_df)
    transformed_df.dropna(subset=["daily_return_pct"], inplace=True)
    dropped = pre_drop - len(transformed_df)
    logger.info(
    f"Dropped {dropped} rows with missing daily_return_pct "
    f"(first row per ticker after pct_change)."
      )

    # ---- STEP 5: Validate each stock has enough rows --------------------─
    thin_stocks = (
        transformed_df.groupby("ticker")
        .size()
        .rename("rows")
        [lambda x: x < MIN_ROWS_THRESHOLD]
    )
    if not thin_stocks.empty:
        logger.warning(
            f"Stocks with fewer than {MIN_ROWS_THRESHOLD} rows after transform "
            f"(may affect rolling quality): {thin_stocks.index.tolist()}"
        )

    # ---- STEP 6: Sort and reset index ------------------------------------------------─
    transformed_df.sort_values(["ticker", "date"], inplace=True)
    transformed_df.reset_index(drop=True, inplace=True)

    # Add country information for Power BI
    country_map = {
    "HDFCBANK.NS": "India",
    "ICICIBANK.NS": "India",
    "SBIN.NS": "India",
    "BAC": "USA",
    "GS": "USA",
    "JPM": "USA",
    "WFC": "USA"
    }

    transformed_df["country"] = transformed_df["ticker"].map(country_map)

    exchange_map = {
    "BAC": "NYSE",
    "GS": "NYSE",
    "JPM": "NYSE",
    "WFC": "NYSE",
    "HDFCBANK.NS": "NSE",
    "ICICIBANK.NS": "NSE",
    "SBIN.NS": "NSE"
    }

    transformed_df["exchange"] = transformed_df["ticker"].map(exchange_map)
    

    # ---- STEP 7: Save output --------------------------------------------------------------------─
    transformed_df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"Saved transformed file : {OUTPUT_FILE}")

    # ---- STEP 8: Summary ----------------------------------------------------------------------------─
    new_cols = [
        "daily_return_pct", f"ma_{MA_SHORT}", f"ma_{MA_MID}", f"ma_{MA_LONG}",
        "volatility_21", "rsi_14", "price_range", "price_range_pct",
        "volume_change_pct", "cumulative_return_pct", "above_ma50",
        "rsi_zone", "signal", "transformed_at"
    ]

    logger.info("")
    logger.info("=" * 60)
    logger.info("TRANSFORMATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Rows before transform  : {rows_before:,}")
    logger.info(f"Rows after transform   : {len(transformed_df):,}")
    logger.info(f"Rows dropped (missing daily_return_pct): {dropped}")
    logger.info(f"New columns added ({len(new_cols)})  : {new_cols}")
    logger.info(f"Output file            : {OUTPUT_FILE}")
    logger.info(f"Log file               : {log_filename}")
    logger.info("=" * 60)
    logger.info("Phase 2 complete. Ready for Phase 3 — Loader (SQLite).")
    logger.info("=" * 60)

    # ---- Quick preview --------------------------------------------------------------------------------─
    print("\n---- SAMPLE OUTPUT (first 5 rows, key columns) ----")
    preview_cols = [
        "ticker", "date", "close", "daily_return_pct",
        f"ma_{MA_SHORT}", f"ma_{MA_MID}", "rsi_14",
        "volatility_21", "signal", "rsi_zone"
    ]
    print(transformed_df[preview_cols].head().to_string(index=False))

    # ---- Per-stock feature summary --------------------------------------------------------─
    print("\n---- PER-STOCK FEATURE SUMMARY ----")
    summary = transformed_df.groupby("ticker").agg(
        trading_days      = ("date",                "count"),
        avg_daily_return  = ("daily_return_pct",    "mean"),
        avg_volatility    = ("volatility_21",        "mean"),
        avg_rsi           = ("rsi_14",               "mean"),
        cumulative_return = ("cumulative_return_pct","last"),
        buy_signals       = ("signal",               lambda x: (x == "Buy").sum()),
        sell_signals      = ("signal",               lambda x: (x == "Sell").sum()),
    ).round(2)
    print(summary.to_string())

    return transformed_df


# ============================================================
# 6. RUN
# ============================================================

if __name__ == "__main__":
    df = run_transformation()

    if df is not None:
        print("\n[SUCCESS] Transformation complete.")
        print(f"[INFO]    Output : data/processed/all_stocks_transformed.csv")
        print(f"[INFO]    Next step : Run phase3_loader.py")
    else:
        print("\n[ERROR] Transformation failed. Check logs for details.")