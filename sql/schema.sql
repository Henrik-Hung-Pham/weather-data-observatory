-- ============================================================================
-- PostgreSQL Schema for Data Observatory - Gold Layer Serving
-- ============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- Core Weather Data Table (Gold Layer)
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold_weather (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Location information
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10) NOT NULL,
    
    -- Temperature data
    temperature_celsius DECIMAL(5,2) NOT NULL,
    feels_like_celsius DECIMAL(5,2),
    
    -- Atmospheric conditions
    humidity INTEGER CHECK (humidity >= 0 AND humidity <= 100),
    pressure INTEGER,
    
    -- Wind data
    wind_speed DECIMAL(6,2),
    wind_direction INTEGER CHECK (wind_direction >= 0 AND wind_direction <= 360),
    
    -- Weather description
    weather_condition VARCHAR(50),
    weather_description VARCHAR(200),
    
    -- Additional metrics
    clouds_percentage INTEGER CHECK (clouds_percentage >= 0 AND clouds_percentage <= 100),
    visibility INTEGER,
    
    -- Timestamps
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    sunrise TIMESTAMP WITH TIME ZONE,
    sunset TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT unique_city_timestamp UNIQUE (city, recorded_at)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_weather_city ON gold_weather(city);
CREATE INDEX IF NOT EXISTS idx_weather_recorded_at ON gold_weather(recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_city_date ON gold_weather(city, DATE(recorded_at));

-- ============================================================================
-- Daily Aggregates Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold_weather_daily (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Location
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    
    -- Temperature aggregates
    temp_avg DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    temp_max DECIMAL(5,2),
    temp_std DECIMAL(5,2),
    
    -- Other aggregates
    humidity_avg DECIMAL(5,2),
    pressure_avg DECIMAL(7,2),
    wind_speed_avg DECIMAL(6,2),
    wind_speed_max DECIMAL(6,2),
    clouds_avg DECIMAL(5,2),
    
    -- Observation count
    observation_count INTEGER,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_city_date UNIQUE (city, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_city_date ON gold_weather_daily(city, date DESC);

-- ============================================================================
-- Pipeline Run History
-- ============================================================================
-- Declared before data_quality_metrics so the latter can reference it.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Run information
    run_id UUID UNIQUE NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed', 'blocked')),

    -- Timing
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,

    -- Processing stats
    cities_processed INTEGER,
    records_ingested INTEGER,
    records_transformed INTEGER,
    records_loaded INTEGER,

    -- Quality gate status
    quality_gate_passed BOOLEAN,
    quality_gate_reason TEXT,

    -- Error information
    error_message TEXT,
    error_traceback TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs(status, started_at DESC);

-- ============================================================================
-- Data Quality Metrics Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Run identification. The FK is what makes "which gate failed on run X?"
    -- answerable; the pipeline stamps every gate with its owning run's id.
    run_id UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    layer VARCHAR(20) NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
    
    -- Quality metrics
    total_records INTEGER NOT NULL,
    passed_records INTEGER NOT NULL,
    failed_records INTEGER NOT NULL,
    pass_rate DECIMAL(5,2) GENERATED ALWAYS AS (
        CASE WHEN total_records > 0 
        THEN (passed_records::DECIMAL / total_records * 100)
        ELSE 0 END
    ) STORED,
    
    -- Expectation details
    expectation_suite VARCHAR(100),
    expectations_evaluated INTEGER,
    expectations_passed INTEGER,
    
    -- Timestamps
    evaluated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Quality gate result
    gate_passed BOOLEAN NOT NULL DEFAULT true,
    failure_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_quality_run ON data_quality_metrics(run_id);
CREATE INDEX IF NOT EXISTS idx_quality_layer ON data_quality_metrics(layer, evaluated_at DESC);

-- ============================================================================
-- Views for Dashboard
-- ============================================================================

-- Latest weather per city
CREATE OR REPLACE VIEW v_latest_weather AS
SELECT DISTINCT ON (city)
    id,
    city,
    country,
    temperature_celsius,
    humidity,
    weather_condition,
    weather_description,
    wind_speed,
    recorded_at,
    ingested_at
FROM gold_weather
ORDER BY city, recorded_at DESC;

-- Quality metrics summary
CREATE OR REPLACE VIEW v_quality_summary AS
SELECT
    layer,
    DATE(evaluated_at) as date,
    COUNT(*) as total_runs,
    AVG(pass_rate) as avg_pass_rate,
    SUM(CASE WHEN gate_passed THEN 1 ELSE 0 END) as gates_passed,
    SUM(CASE WHEN NOT gate_passed THEN 1 ELSE 0 END) as gates_failed
FROM data_quality_metrics
GROUP BY layer, DATE(evaluated_at)
ORDER BY date DESC, layer;

-- Recent pipeline runs
CREATE OR REPLACE VIEW v_recent_runs AS
SELECT
    run_id,
    status,
    started_at,
    completed_at,
    duration_seconds,
    cities_processed,
    records_loaded,
    quality_gate_passed,
    error_message
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 50;
