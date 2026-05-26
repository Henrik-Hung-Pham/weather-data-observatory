"""CLI entry point for the Data Observatory."""

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
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
    elif args.command == "dashboard":
        return run_dashboard()
    elif args.command == "validate":
        return run_validation(args)
    elif args.command == "init":
        return init_database()
    else:
        parser.print_help()
        return 0


def run_pipeline(args) -> int:
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


def run_validation(args) -> int:
    """Run data validation for a specific layer."""
    from data_pipeline.quality.validator import DataValidator
    from data_pipeline.storage import DataLakeStorage

    logger.info(f"Running validation for {args.layer} layer...")

    try:
        storage = DataLakeStorage()
        validator = DataValidator()

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

        suite_name = f"{args.layer}_weather_suite"
        result = validator.validate(data, suite_name)

        if result["success"]:
            logger.info(f"✅ Validation passed: {result['statistics']}")
            return 0
        else:
            logger.error(f"❌ Validation failed: {result['statistics']}")
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
