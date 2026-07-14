"""Schema consistency guards.

``data_pipeline/schema.py`` is the single source of truth for the medallion
column sets. The Python call sites import from it directly, so they cannot
drift. These tests guard the remaining definitions that *cannot* import the
schema module — the dataclass shape and the SQL DDL — so that a column change
which forgets one of them fails CI instead of silently shipping.
"""

from datetime import datetime, timezone
from pathlib import Path

from data_pipeline.ingestion.weather_api import WeatherData
from data_pipeline.pipeline import DataPipeline
from data_pipeline.schema import (
    BRONZE_COLUMNS,
    BRONZE_SCHEMA,
    GOLD_SERVING_COLUMNS,
    SILVER_COLUMNS,
    SILVER_SCHEMA,
)
from data_pipeline.transformation.silver import SilverTransformer

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_SQL = REPO_ROOT / "sql" / "schema.sql"


def _sample_weather() -> WeatherData:
    now = datetime.now(timezone.utc)
    return WeatherData(
        city="London",
        country="GB",
        temperature_kelvin=290.0,
        temperature_celsius=16.85,
        feels_like_celsius=16.0,
        humidity=70,
        pressure=1012,
        wind_speed=3.5,
        wind_direction=200,
        weather_condition="Clouds",
        weather_description="overcast clouds",
        clouds_percentage=90,
        visibility=10000,
        timestamp=now,
        sunrise=now,
        sunset=now,
        raw_response={},
    )


def test_weather_dataclass_matches_bronze_schema() -> None:
    """WeatherData.to_dict() must produce exactly the Bronze column set."""
    keys = set(_sample_weather().to_dict().keys())
    assert keys == BRONZE_SCHEMA


def test_pipeline_schemas_come_from_canonical_module() -> None:
    assert DataPipeline.BRONZE_SCHEMA == BRONZE_SCHEMA
    assert DataPipeline.SILVER_SCHEMA == SILVER_SCHEMA


def test_silver_transformer_schema_matches() -> None:
    assert set(SilverTransformer.SCHEMA.keys()) == SILVER_SCHEMA


def test_silver_clean_record_matches_schema() -> None:
    """The actual cleaned record keys must match the Silver column set."""
    transformer = SilverTransformer.__new__(SilverTransformer)
    cleaned = transformer._clean_record(_sample_weather().to_dict())
    assert set(cleaned.keys()) == SILVER_SCHEMA


def test_gold_table_ddl_has_all_serving_columns() -> None:
    """sql/schema.sql gold_weather must define every Gold serving column."""
    ddl = SCHEMA_SQL.read_text()
    start = ddl.index("CREATE TABLE IF NOT EXISTS gold_weather (")
    end = ddl.index(");", start)
    block = ddl[start:end]
    for column in GOLD_SERVING_COLUMNS:
        # word-boundary-ish check: column name followed by a space/type
        assert f"    {column} " in block, f"gold_weather DDL missing column: {column}"


def test_silver_columns_are_bronze_minus_raw_plus_metadata() -> None:
    """Document the intended Bronze->Silver column relationship."""
    dropped = set(BRONZE_COLUMNS) - set(SILVER_COLUMNS)
    added = set(SILVER_COLUMNS) - set(BRONZE_COLUMNS)
    assert dropped == {"temperature_kelvin", "ingested_at"}
    assert added == {"_transformed_at", "_source_layer"}
