"""Dagster orchestration for the medallion pipeline.

Wraps the existing :class:`~data_pipeline.pipeline.DataPipeline` as a Dagster
asset so the pipeline gains scheduling, a run UI, retries, and run history
without duplicating any ETL logic.

This module is optional — Dagster is an extra dependency
(``pip install -e ".[orchestration]"``). It is intentionally **not** imported
by ``data_pipeline/__init__.py`` so the core package has no hard dependency on
Dagster.

Run locally:

    dagster dev -m data_pipeline.orchestration.definitions

The schedule cron defaults to hourly and can be overridden with the
``DAGSTER_CRON`` environment variable.
"""

import os
from typing import Any

from dagster import (
    AssetExecutionContext,
    Definitions,
    Failure,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from data_pipeline.pipeline import DataPipeline

DEFAULT_CRON = os.getenv("DAGSTER_CRON", "0 * * * *")  # hourly


@asset(
    group_name="medallion",
    description="Runs the full Bronze->Silver->Gold weather pipeline with quality gates.",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
)
def weather_observatory(context: AssetExecutionContext) -> MaterializeResult[Any]:
    """Execute one pipeline run and surface its result to Dagster.

    A ``success`` run materializes with metadata; a ``failed`` or ``blocked``
    run raises :class:`dagster.Failure` so it shows red in the UI and triggers
    Dagster's retry/alerting machinery.
    """
    result = DataPipeline().run()

    metadata = {
        "run_id": MetadataValue.text(str(result.run_id)),
        "status": MetadataValue.text(result.status),
        "records_ingested": MetadataValue.int(result.records_ingested),
        "records_transformed": MetadataValue.int(result.records_transformed),
        "records_loaded": MetadataValue.int(result.records_loaded),
        "duration_seconds": MetadataValue.float(round(result.duration_seconds, 2)),
        "quality_gate_passed": MetadataValue.bool(result.quality_gate_passed),
    }

    if result.status != "success":
        reason = result.quality_gate_reason or result.error_message or result.status
        context.log.error(f"Pipeline {result.status}: {reason}")
        raise Failure(description=f"Pipeline {result.status}: {reason}", metadata=metadata)

    context.log.info(f"Pipeline succeeded: loaded {result.records_loaded} records")
    return MaterializeResult(metadata=metadata)


weather_pipeline_job = define_asset_job(
    name="weather_pipeline_job",
    selection=[weather_observatory],
)

weather_schedule = ScheduleDefinition(
    name="weather_pipeline_schedule",
    job=weather_pipeline_job,
    cron_schedule=DEFAULT_CRON,
)

defs = Definitions(
    assets=[weather_observatory],
    jobs=[weather_pipeline_job],
    schedules=[weather_schedule],
)
