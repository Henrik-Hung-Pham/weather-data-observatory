"""Guard: every third-party module we import must be a declared dependency.

`dashboard/app.py` imported plotly, which appeared in neither
`requirements.txt` nor `pyproject.toml`. Nothing caught it: CI never builds
the dashboard image and never imports the dashboard, so the failure only
surfaced at `docker-compose up`, in the flow the README calls
"Option 1 (Recommended)".

This test walks the imports of the shipped application code and fails if one
resolves to a distribution we never declared.
"""

import ast
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Import name -> distribution name, where they differ.
_MODULE_TO_DISTRIBUTION = {
    "dotenv": "python-dotenv",
    "pydantic_settings": "pydantic-settings",
    "sqlalchemy": "sqlalchemy",
    "psycopg2": "psycopg2-binary",
}

# Modules that legitimately need no direct declaration.
_FIRST_PARTY = {"data_pipeline", "dashboard"}
_EXEMPT = {
    # Pulled in and pinned by requests; we only touch its Retry helper.
    "urllib3",
    # boto3 pins botocore to an exact-ish range and they must move together,
    # so declaring botocore separately invites a resolver conflict. Importing
    # ClientError/Config from it directly is the standard boto3 idiom.
    "botocore",
    # Optional extra, declared under [project.optional-dependencies].
    "dagster",
}


def _declared_distributions() -> set[str]:
    """Distribution names declared as runtime dependencies in pyproject."""
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    deps = pyproject["project"]["dependencies"]
    # "pandas>=2.0.0" -> "pandas"
    return {
        dep.split(">")[0].split("<")[0].split("=")[0].split("[")[0].strip().lower() for dep in deps
    }


def _imported_modules(path: Path) -> set[str]:
    """Top-level module names imported by a Python file."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            # `from . import x` has no module; it is first-party by definition.
            if node.level == 0 and node.module:
                modules.add(node.module.split(".")[0])
    return modules


def _application_files() -> list[Path]:
    return sorted(
        [*(REPO_ROOT / "data_pipeline").rglob("*.py"), *(REPO_ROOT / "dashboard").rglob("*.py")]
    )


@pytest.mark.unit
def test_every_imported_third_party_module_is_declared() -> None:
    declared = _declared_distributions()
    undeclared: dict[str, str] = {}

    for path in _application_files():
        for module in _imported_modules(path):
            if module in sys.stdlib_module_names or module in _FIRST_PARTY or module in _EXEMPT:
                continue
            distribution = _MODULE_TO_DISTRIBUTION.get(module, module).lower()
            if distribution not in declared:
                undeclared[module] = str(path.relative_to(REPO_ROOT))

    assert not undeclared, (
        "Imported but not declared in pyproject [project.dependencies]: "
        + ", ".join(f"{mod} (in {loc})" for mod, loc in sorted(undeclared.items()))
    )


@pytest.mark.unit
def test_requirements_txt_matches_pyproject_runtime_deps() -> None:
    """The two dependency lists must not drift apart.

    The Docker images install requirements.txt while `pip install -e .` uses
    pyproject, so a dependency present in only one of them produces an image
    that behaves differently from a local checkout.
    """
    requirements = {
        line.split(">")[0].split("<")[0].split("=")[0].strip().lower()
        for line in (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }

    missing_from_requirements = _declared_distributions() - requirements
    assert not missing_from_requirements, (
        f"Declared in pyproject but absent from requirements.txt "
        f"(so missing from the Docker images): {sorted(missing_from_requirements)}"
    )
