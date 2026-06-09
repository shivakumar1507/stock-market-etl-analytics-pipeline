-- Portfolio KPIs: latest snapshot per stock
SELECT
    sp.ticker,
    sm.company_name,
    sm.exchange,
    sm.country,
    sp.close,
    sp.cumulative_return_pct,
    sp.volatility_21,
    sp.rsi_14,
    sp.signal
FROM stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.date = (SELECT MAX(date) FROM stock_prices WHERE ticker = sp.ticker)
ORDER BY sp.cumulative_return_pct DESC;

-- Best performers by cumulative return
SELECT
    sp.ticker,
    sm.company_name,
    sm.country,
    sm.exchange,
    sp.cumulative_return_pct
FROM stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.date = (SELECT MAX(date) FROM stock_prices WHERE ticker = sp.ticker)
ORDER BY sp.cumulative_return_pct DESC;

-- Worst performers by cumulative return
SELECT
    sp.ticker,
    sm.company_name,
    sm.country,
    sm.exchange,
    sp.cumulative_return_pct
FROM stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.date = (SELECT MAX(date) FROM stock_prices WHERE ticker = sp.ticker)
ORDER BY sp.cumulative_return_pct ASC;

-- Current RSI signals with exchange info
SELECT
    sp.ticker,
    sm.company_name,
    sm.exchange,
    sm.country,
    sp.rsi_14,
    sp.rsi_zone,
    sp.signal
FROM stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.date = (SELECT MAX(date) FROM stock_prices WHERE ticker = sp.ticker)
ORDER BY sp.rsi_14 DESC;

-- Volatility ranking with sector context
SELECT
    sp.ticker,
    sm.company_name,
    sm.country,
    sm.sector,
    sp.volatility_21
FROM stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.date = (SELECT MAX(date) FROM stock_prices WHERE ticker = sp.ticker)
ORDER BY sp.volatility_21 DESC;

-- India vs USA: cumulative return comparison
SELECT
    sm.country,
    ROUND(AVG(sp.cumulative_return_pct), 2) AS avg_return_pct,
    ROUND(MIN(sp.cumulative_return_pct), 2) AS min_return_pct,
    ROUND(MAX(sp.cumulative_return_pct), 2) AS max_return_pct
FROM stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.date = (SELECT MAX(date) FROM stock_prices WHERE ticker = sp.ticker)
GROUP BY sm.country;

-- India vs USA: volatility comparison
SELECT
    sm.country,
    ROUND(AVG(sp.volatility_21), 4) AS avg_volatility,
    ROUND(MIN(sp.volatility_21), 4) AS min_volatility,
    ROUND(MAX(sp.volatility_21), 4) AS max_volatility
FROM stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.date = (SELECT MAX(date) FROM stock_prices WHERE ticker = sp.ticker)
GROUP BY sm.country;

-- RSI zone distribution by country
SELECT
    sm.country,
    sp.rsi_zone,
    COUNT(*) AS day_count
FROM stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.rsi_zone IS NOT NULL
GROUP BY sm.country, sp.rsi_zone
ORDER BY sm.country, day_count DESC;

-- Monthly avg daily return trend: India vs USA
SELECT
    sm.country,
    strftime('%Y-%m', sp.date) AS month,
    ROUND(AVG(sp.daily_return_pct), 4) AS avg_daily_return
FROM stock_prices sp
JOIN stock_metadata sm
    ON sp.ticker = sm.ticker
GROUP BY
    sm.country,
    strftime('%Y-%m', sp.date)
ORDER BY
    sm.country,
    month;

-- Buy vs Sell signal counts per ticker
SELECT
    sp.ticker,
    sm.company_name,
    sm.country,
    sm.exchange,
    sp.signal,
    COUNT(*) AS signal_count
FROM stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.signal IN ('Buy', 'Sell')
GROUP BY sp.ticker, sm.company_name, sm.country, sm.exchange, sp.signal
ORDER BY sp.ticker, sp.signal;

SELECT
    sp.ticker,
    sm.company_name,
    sm.country,
    sp.close AS latest_close,
    sm.avg_close AS historical_avg_close,
    ROUND((sp.close - sm.avg_close) / sm.avg_close * 100, 2) AS pct_vs_avg,
    (
        SELECT COUNT(*)
        FROM stock_prices s2
        WHERE s2.ticker = sp.ticker
    ) AS trading_days
FROM stock_prices sp
JOIN stock_metadata sm
    ON sp.ticker = sm.ticker
WHERE sp.date = (
    SELECT MAX(date)
    FROM stock_prices
    WHERE ticker = sp.ticker
)
ORDER BY pct_vs_avg DESC;

-- Top 3 most volatile days per stock (window function)
SELECT ticker, date, daily_return_pct, volatility_21
FROM (
    SELECT
        ticker, date, daily_return_pct, volatility_21,
        RANK() OVER (PARTITION BY ticker ORDER BY ABS(daily_return_pct) DESC) AS rnk
    FROM stock_prices
    WHERE daily_return_pct IS NOT NULL
)
WHERE rnk <= 3
ORDER BY ticker, rnk;

-- Days since last signal change per ticker (shows momentum)
SELECT
    ticker,
    signal,
    date,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS days_in_current_signal
FROM stock_prices
WHERE date = (SELECT MAX(date) FROM stock_prices WHERE ticker = stock_prices.ticker)
   OR signal != LAG(signal) OVER (PARTITION BY ticker ORDER BY date)
ORDER BY ticker;