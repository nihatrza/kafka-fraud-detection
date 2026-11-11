-- Target table for analytical serving layer
-- Note: Raw transaction vectors (30+ features) are processed in-memory by LightGBM
-- Only high-value business & risk indicators are persisted below for analytical query performance.

DROP TABLE IF EXISTS real_time_fraud_predictions;

CREATE TABLE real_time_fraud_predictions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'USD',
    merchant_category VARCHAR(100) NOT NULL,
    state VARCHAR(10) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    network_type VARCHAR(50) NOT NULL,
    predicted_is_fraud INT NOT NULL CHECK (predicted_is_fraud IN (0, 1)),
    predicted_fraud_probability NUMERIC(6, 4) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Performance Optimization Indexes
CREATE INDEX idx_fraud_predictions_state ON real_time_fraud_predictions(state);
CREATE INDEX idx_fraud_predictions_category ON real_time_fraud_predictions(merchant_category);
CREATE INDEX idx_fraud_predictions_is_fraud ON real_time_fraud_predictions(predicted_is_fraud);
