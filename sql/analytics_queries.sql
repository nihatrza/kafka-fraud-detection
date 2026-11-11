-- ===================================================================
-- Real-Time Fraud Analytics Queries (PostgreSQL)
-- ===================================================================

-- 1. Top 5 High-Risk US States by Fraud Frequency and Financial Impact
SELECT 
    state,
    COUNT(*) AS total_transactions,
    COUNT(*) FILTER (WHERE predicted_is_fraud = 1) AS fraud_count,
    ROUND((COUNT(*) FILTER (WHERE predicted_is_fraud = 1)::NUMERIC / COUNT(*)) * 100, 2) AS fraud_rate_pct,
    ROUND(SUM(CASE WHEN predicted_is_fraud = 1 THEN amount ELSE 0 END)::NUMERIC, 2) AS total_blocked_amount_usd
FROM real_time_fraud_predictions
GROUP BY state
ORDER BY fraud_count DESC
LIMIT 5;

-- 2. Risk Density by Merchant Category
SELECT 
    merchant_category,
    COUNT(*) AS total_txns,
    COUNT(*) FILTER (WHERE predicted_is_fraud = 1) AS fraud_txns,
    ROUND(AVG(CASE WHEN predicted_is_fraud = 1 THEN amount END)::NUMERIC, 2) AS avg_fraud_amount_usd,
    ROUND(AVG(predicted_fraud_probability)::NUMERIC, 4) AS avg_risk_score
FROM real_time_fraud_predictions
GROUP BY merchant_category
ORDER BY fraud_txns DESC
LIMIT 10;

-- 3. Risk Matrix across Device Reliability and Network Vectors (TOR / VPN / Cellular)
SELECT 
    device_type,
    network_type,
    COUNT(*) AS total_txns,
    COUNT(*) FILTER (WHERE predicted_is_fraud = 1) AS fraud_cases,
    ROUND((COUNT(*) FILTER (WHERE predicted_is_fraud = 1)::NUMERIC / COUNT(*)) * 100, 2) AS risk_pct
FROM real_time_fraud_predictions
GROUP BY device_type, network_type
ORDER BY risk_pct DESC;