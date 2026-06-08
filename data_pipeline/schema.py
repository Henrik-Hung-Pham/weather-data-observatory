"""Canonical schema definitions for the medallion layers.

Single source of truth for the weather data schema across every layer.

Historically the column set was duplicated in six places — the
``WeatherData`` dataclass, the ``BRONZE_SCHEMA`` / ``SILVER_SCHEMA``
frozensets in ``pipeline.py``, ``SilverTransformer.SCHEMA``, the validator
defaults, the Great Expectations JSON suites, and ``sql/schema.sql``. A
single column change had to be mirrored by hand in all of them, which is
error prone and was the project's most painful cross-cutting concern.

This module defines the schema once. The Python call sites import their
frozensets / type maps directly from here, and a consistency test
(``tests/unit/test_schema_consistency.py``) guards the two external
artifacts that cannot import Python: the GE JSON suites and the SQL DDL.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Field:
    """A single column in a medallion layer schema."""

    name: str
    py_type: type
    metadata: bool = False  # underscore-prefixed pipeline metadata field


# --- Bronze: raw fields exactly as produced by WeatherData.to_dict() ---
BRONZE_FIELDS: tuple[Field, ...] = (
    Field("city", str),
    Field("country", str),
    Field("temperature_kelvin", float),
    Field("temperature_celsius", float),
    Field("feels_like_celsius", float),
    Field("humidity", int),
    Field("pressure", int),
    Field("wind_speed", float),
    Field("wind_direction", int),
    Field("weather_condition", str),
    Field("weather_description", str),
    Field("clouds_percentage", int),
    Field("visibility", int),
    Field("timestamp", str),
    Field("sunrise", str),
    Field("sunset", str),
    Field("ingested_at", str),
)

# --- Silver: cleaned fields produced by SilverTransformer._clean_record() ---
# Drops the raw Kelvin temperature and ingested_at; adds pipeline metadata.
SILVER_FIELDS: tuple[Field, ...] = (
    Field("city", str),
    Field("country", str),
    Field("temperature_celsius", float),
    Field("feels_like_celsius", float),
    Field("humidity", int),
    Field("pressure", int),
    Field("wind_speed", float),
    Field("wind_direction", int),
    Field("weather_condition", str),
    Field("weather_description", str),
    Field("clouds_percentage", int),
    Field("visibility", int),
    Field("timestamp", str),
    Field("sunrise", str),
    Field("sunset", str),
    Field("_transformed_at", str, metadata=True),
    Field("_source_layer", str, metadata=True),
)

# --- Gold: serving columns persisted to the gold_weather table ---
# The Silver ``timestamp`` is stored as ``recorded_at``; metadata fields are
# dropped and ``ingested_at`` is (re)stamped at load time.
GOLD_SERVING_COLUMNS: tuple[str, ...] = (
    "city",
    "country",
    "temperature_celsius",
    "feels_like_celsius",
    "humidity",
    "pressure",
    "wind_speed",
    "wind_direction",
    "weather_condition",
    "weather_description",
    "clouds_percentage",
    "visibility",
    "recorded_at",
    "sunrise",
    "sunset",
    "ingested_at",
)

# --- Derived views consumed across the pipeline ---
BRONZE_COLUMNS: tuple[str, ...] = tuple(f.name for f in BRONZE_FIELDS)
SILVER_COLUMNS: tuple[str, ...] = tuple(f.name for f in SILVER_FIELDS)

BRONZE_SCHEMA: frozenset[str] = frozenset(BRONZE_COLUMNS)
SILVER_SCHEMA: frozenset[str] = frozenset(SILVER_COLUMNS)

SILVER_FIELD_TYPES: dict[str, type] = {f.name: f.py_type for f in SILVER_FIELDS}
