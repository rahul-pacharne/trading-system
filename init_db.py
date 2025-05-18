import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Database connection parameters
db_params = {
    "dbname": "prompt_trader",
    "user": "trader",
    "password": "secure_password",
    "host": "localhost",
    "port": "5432"
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

# Enable TimescaleDB extension
cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

# Create market data table for index and options
cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_data (
        time TIMESTAMPTZ NOT NULL,
        instrument_key TEXT NOT NULL,
        ltp DOUBLE PRECISION,
        volume BIGINT,
        last_trade_time BIGINT,
        last_close DOUBLE PRECISION,
        strike_price DOUBLE PRECISION,
        option_type TEXT,  -- 'CE' for Call, 'PE' for Put
        open_interest BIGINT,
        expiry_date DATE,
        PRIMARY KEY (time, instrument_key)
    );
""")

# Convert market_data to TimescaleDB hypertable
cursor.execute("SELECT create_hypertable('market_data', 'time', if_not_exists => TRUE);")

# Create index for market_data queries
cursor.execute("CREATE INDEX IF NOT EXISTS idx_instrument_key ON market_data (instrument_key, time DESC);")

# Create trading signals table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS trading_signals (
        signal_time TIMESTAMPTZ NOT NULL,
        instrument_key TEXT NOT NULL,
        signal_type TEXT NOT NULL,  -- 'BUY' or 'SELL'
        ltp DOUBLE PRECISION,
        rsi DOUBLE PRECISION,
        macd DOUBLE PRECISION,
        atr DOUBLE PRECISION,
        PRIMARY KEY (signal_time, instrument_key)
    );
""")

# Convert trading_signals to TimescaleDB hypertable
cursor.execute("SELECT create_hypertable('trading_signals', 'signal_time', if_not_exists => TRUE);")

print("Database schema created successfully")
cursor.close()
conn.close()