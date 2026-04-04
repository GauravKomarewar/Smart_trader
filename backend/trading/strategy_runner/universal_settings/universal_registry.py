from pathlib import Path
from typing import Dict, List


def list_strategy_templates() -> List[Dict[str, str]]:
    """
    Discover strategy templates from strategy_runner python files.
    """
    base = Path(__file__).resolve().parents[1]
    templates: List[Dict[str, str]] = []
    excluded = {
        "__init__.py",
        "models.py",
        "state.py",
        "config_schema.py",
        "condition_engine.py",
        "entry_engine.py",
        "adjustment_engine.py",
        "exit_engine.py",
        "market_reader.py",
        "persistence.py",
        "reconciliation.py",
        "executor.py",
        "strategy_executor_service.py",
        "simulation_harness.py",
    }
    for file in sorted(base.glob("*.py")):
        if file.name in excluded:
            continue
        slug = file.stem
        templates.append(
            {
                "id": slug.upper(),
                "folder": "strategy_runner",
                "file": file.name,
                "module": f"trading.strategy_runner.{slug}",
                "label": slug.replace("_", " ").title(),
                "slug": slug,
            }
        )
    return templates

