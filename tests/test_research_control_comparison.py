"""Exercise the real closeout guard without importing or running the campaign."""

import ast
import copy
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

import pytest


@pytest.fixture
def closeout_guard() -> Callable[[object, object], None]:
    # The campaign lives outside an importable package and imports operational tools.
    # Compile only its actual require() definition and closeout guard; no I/O/tasks.
    path = Path(__file__).resolve().parents[1] / "data/clean-stage-b-tools-20260909/campaign.py"
    tree = ast.parse(path.read_text())
    require = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "require"
    )
    equality = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "research_controls_equal"
    )
    guards = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "require"
        and len(node.args) == 2
        and isinstance(node.args[1], ast.Constant)
        and node.args[1].value == "research controls changed"
    ]
    assert len(guards) == 1
    function = ast.parse("def check(batch, config):\n    pass\n").body[0]
    assert isinstance(function, ast.FunctionDef)
    function.body = [ast.Expr(value=guards[0])]
    module = ast.fix_missing_locations(
        ast.Module(body=[require, equality, function], type_ignores=[])
    )
    namespace: dict[str, object] = {"json": json}
    exec(compile(module, str(path), "exec"), namespace)
    check = cast(Callable[[object, object], None], namespace["check"])

    def invoke(actual: object, expected: object) -> None:
        check({"configuration": actual}, {"research_controls": {"configuration": expected}})

    return invoke


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ({"cost_grid_bps": (0, 0.5, 1)}, {"cost_grid_bps": [0, 0.5, 1]}),
        ({"nested": ({"values": (1, (2, 3))},)}, {"nested": [{"values": [1, [2, 3]]}]}),
        ({"seed": 42, "nested": {"a": 1, "b": 2}}, {"nested": {"b": 2, "a": 1}, "seed": 42}),
    ],
    ids=["tuple-list", "nested-tuples", "dictionary-order"],
)
def test_equivalent_json_values(
    closeout_guard: Callable[[object, object], None], left: object, right: object
) -> None:
    before = copy.deepcopy((left, right))
    closeout_guard(left, right)
    assert (left, right) == before


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ([0, 0.5, 1], [0, 0.6, 1]),
        ([0, 0.5, 1], [1, 0.5, 0]),
        ({"block_length_ms": 10000}, {}),
        ({"threshold": 0.6}, {"threshold": 0.61}),
        ({"horizons_ms": (250, 500, 1000)}, {"horizons_ms": [250, 500, 2000]}),
        ({"bootstrap_replications": 200}, {"bootstrap_replications": 201}),
        ({"seed": 42}, {"seed": 43}),
        ({"cost_grid_bps": (0, 0.5, 1)}, {"cost_grid_bps": [0, 0.6, 1]}),
        ({"seed": "42"}, {"seed": 42}),
        ({"threshold": 0.5}, {"threshold": 0.5000000000000001}),
        ({"optional": None}, {}),
        ({"enabled": True}, {"enabled": 1}),
    ],
    ids=[
        "numeric-change",
        "sequence-order",
        "missing-key",
        "strategy-threshold",
        "horizon",
        "bootstrap-replications",
        "seed",
        "cost-grid",
        "string-not-number",
        "no-rounding",
        "null-not-missing",
        "boolean-not-number",
    ],
)
def test_actual_changes_fail_closed(
    closeout_guard: Callable[[object, object], None], left: object, right: object
) -> None:
    with pytest.raises(RuntimeError, match="^research controls changed$"):
        closeout_guard(left, right)


def test_dataclass_after_asdict(closeout_guard: Callable[[object, object], None]) -> None:
    @dataclass
    class Controls:
        cost_grid_bps: tuple[float, ...] = (0.0, 0.5, 1.0)
        seed: int = 42

    values = asdict(Controls())
    closeout_guard(json.loads(json.dumps(values)), values)


def test_september_9_closeout_shape(closeout_guard: Callable[[object, object], None]) -> None:
    # Immutable fixture of the complete compared object; never reads historical artifacts.
    values = {
        "block_length_ms": 10000,
        "bootstrap_replications": 200,
        "cost_grid_bps": (0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 10.0, 12.0),
        "maker_taker_cost_bps": 8.0,
        "max_hour_features": 100000,
        "seed": 42,
        "taker_taker_cost_bps": 11.0,
    }
    persisted = json.loads(json.dumps(values))
    # argparse's existing CLI defaults serialize these equal numbers as integers.
    persisted["maker_taker_cost_bps"] = 8
    persisted["taker_taker_cost_bps"] = 11
    assert persisted != values  # Exact former failure shape.
    closeout_guard(persisted, values)
    persisted["cost_grid_bps"][1] = 0.6
    with pytest.raises(RuntimeError, match="^research controls changed$"):
        closeout_guard(persisted, values)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_values_fail_closed(
    closeout_guard: Callable[[object, object], None], value: float
) -> None:
    with pytest.raises(ValueError):
        closeout_guard({"cost": value}, {"cost": value})


def test_large_integer_difference_not_lost(
    closeout_guard: Callable[[object, object], None],
) -> None:
    with pytest.raises(RuntimeError, match="^research controls changed$"):
        closeout_guard({"seed": 2**53 + 1}, {"seed": float(2**53)})
