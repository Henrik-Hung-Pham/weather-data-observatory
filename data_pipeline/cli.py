"""CLI entry point for the Data Observatory."""

import argparse
import logging
import sys
from datetime import datetime, timezone

from data_pipeline.config import get_settings
from data_pipeline.logging_config import configure_logging

_settings = get_settings()
configure_logging(_settings.log_level, _settings.log_format)
logger = logging.getLogger(__name__)


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Data Observatory - Self-Healing Data Quality Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run pipeline command
    run_parser = subparsers.add_parser("run", help="Run the data pipeline")
    run_parser.add_argument(
        "--cities",
        type=str,
        help="Comma-separated list of cities to fetch weather for",
    )
    run_parser.add_argument(
        "--quality-mode",
        choices=["warn", "block"],
        default="block",
        help="Quality gate mode (default: block)",
    )

    # Replay command
    replay_parser = subparsers.add_parser(
        "replay",
        help="Re-run Silver and Gold over Bronze data already in the data lake",
        description=(
            "Replay a range of Bronze partitions through the Silver and Gold "
            "stages without calling the weather API. Use this to apply a "
            "transformation fix to history: re-fetching is not an option for a "
            "current-weather source, which would return today's readings "
            "rather than the day being repaired. The same three quality gates "
            "run as in a live run, and the serving-layer writes upsert, so "
            "replaying a range twice is safe."
        ),
    )
    replay_parser.add_argument(
        "--from",
        dest="from_date",
        type=_utc_date,
        required=True,
        metavar="YYYY-MM-DD",
        help="First Bronze partition date to replay (inclusive)",
    )
    replay_parser.add_argument(
        "--to",
        dest="to_date",
        type=_utc_date,
        metavar="YYYY-MM-DD",
        help="Last Bronze partition date to replay (inclusive; defaults to --from)",
    )

    # Dashboard command
    subparsers.add_parser("dashboard", help="Start the Streamlit dashboard")

    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Run data validation only")
    validate_parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        required=True,
        help="Data layer to validate",
    )

    # Init command
    subparsers.add_parser("init", help="Initialize the database schema")

    args = parser.parse_args()

    if args.command == "run":
        return run_pipeline(args)
    elif args.command == "replay":
        return replay_pipeline(args)
    elif args.command == "dashboard":
        return run_dashboard()
    elif args.command == "validate":
        return run_validation(args)
    elif args.command == "init":
        return init_database()
    else:
        parser.print_help()
        return 0


def _utc_date(value: str) -> datetime:
    """Parse a ``YYYY-MM-DD`` CLI argument into a UTC datetime.

    Raises:
        argparse.ArgumentTypeError: If the value is not an ISO date, so the
            user gets argparse's usage message rather than a traceback.
    """
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a date as YYYY-MM-DD, got {value!r}") from None


def replay_pipeline(args: argparse.Namespace) -> int:
    """Replay Bronze partitions through Silver and Gold."""
    from data_pipeline.pipeline import DataPipeline

    if args.to_date and args.to_date < args.from_date:
        logger.error("--to must not be earlier than --from")
        return 1

    try:
        pipeline = DataPipeline()
        result = pipeline.replay(start_date=args.from_date, end_date=args.to_date)

        if result.status == "success":
            logger.info(
                f"✅ Replay completed: {result.records_ingested} Bronze records "
                f"-> {result.records_loaded} loaded"
            )
            return 0
        elif result.status == "blocked":
            logger.error(f"🛑 Replay blocked: {result.quality_gate_reason}")
            return 2
        else:
            logger.error(f"❌ Replay failed: {result.error_message}")
            return 1

    except Exception as e:
        logger.error(f"Replay failed: {e}")
        return 1


def run_pipeline(args: argparse.Namespace) -> int:
    """Run the data pipeline."""
    from data_pipeline.pipeline import DataPipeline

    cities = None
    if args.cities:
        cities = [c.strip() for c in args.cities.split(",")]

    try:
        pipeline = DataPipeline()
        result = pipeline.run(cities=cities)

        if result.status == "success":
            logger.info("✅ Pipeline completed successfully")
            return 0
        elif result.status == "blocked":
            logger.error(f"🛑 Pipeline blocked: {result.quality_gate_reason}")
            return 2
        else:
            logger.error(f"❌ Pipeline failed: {result.error_message}")
            return 1

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return 1


def run_dashboard() -> int:
    """Start the Streamlit dashboard."""
    import subprocess

    logger.info("Starting Streamlit dashboard...")

    try:
        subprocess.run(
            ["streamlit", "run", "dashboard/app.py"],
            check=True,
        )
        return 0
    except subprocess.CalledProcessError as e:
        logger.error(f"Dashboard failed: {e}")
        return 1
    except KeyboardInterrupt:
        logger.info("Dashboard stopped")
        return 0


def run_validation(args: argparse.Namespace) -> int:
    """Run the quality gate for a specific layer against its latest data."""
    from data_pipeline.quality.gates import build_gate_for_layer
    from data_pipeline.storage import DataLakeStorage

    logger.info(f"Running validation for {args.layer} layer...")

    try:
        storage = DataLakeStorage()

        # Get latest data from layer
        keys = storage.list_objects(args.layer)

        if not keys:
            logger.warning(f"No data found in {args.layer} layer")
            return 0

        # Read and validate latest file
        latest_key = sorted(keys)[-1]
        data = storage.read_json(latest_key)

        if not isinstance(data, list):
            data = [data]

        gate = build_gate_for_layer(args.layer)
        result = gate.evaluate(data, args.layer)

        if result.passed:
            logger.info(f"✅ Validation passed: {result.metrics}")
            return 0
        else:
            logger.error(f"❌ Validation blocked: {result.metrics}")
            return 1

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return 1


def init_database() -> int:
    """Initialize the database schema."""
    from data_pipeline.storage import DatabaseManager

    logger.info("Initializing database schema...")

    try:
        db = DatabaseManager()

        if db.initialize_schema():
            logger.info("✅ Database schema initialized successfully")
            return 0
        else:
            logger.warning("Database schema initialization had warnings")
            return 0

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
