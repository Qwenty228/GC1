"""Load an experiment file: the one place a person plugs in their model.

An experiment file is a plain Python file (see `evaluation/consolidation/experiments/`)
that defines:

    NAME = "my-model"                       # short, used in result file names
    DESCRIPTION = "what this tries"         # one line, stored with the results
    PAID_CALLS = True                       # True if running it costs money (asks for --yes)

    def build_comparator(settings):         # REQUIRED for consolidation
        return MyComparator()               # an object with .name, .model, .compare(request)

    def build_embedder(settings):           # OPTIONAL: swap the embedding model too
        ...

`settings` is the project's SimilaritySettings (from .env), for experiments that want
its values; ignoring it is fine.
"""

from __future__ import annotations

import importlib.util
import re
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Callable


class ExperimentError(ValueError):
    pass


@dataclass(frozen=True)
class Experiment:
    path: Path
    name: str
    description: str
    paid_calls: bool
    module: ModuleType

    def hook(self, name: str) -> Callable[..., Any] | None:
        fn = getattr(self.module, name, None)
        return fn if callable(fn) else None


def load_experiment(path: str | Path, *, required: tuple[str, ...] = ("build_comparator",)) -> Experiment:
    path = Path(path).resolve()
    if not path.is_file() or path.suffix != ".py":
        raise ExperimentError(f"{path}: not a .py experiment file")
    spec = importlib.util.spec_from_file_location(f"experiment_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # the user's file: report its error, whatever it is
        raise ExperimentError(f"{path.name} failed to import: {exc!r}") from exc
    missing = [name for name in required if not callable(getattr(module, name, None))]
    if missing:
        raise ExperimentError(f"{path.name} must define {', '.join(f'{m}(settings)' for m in missing)}")
    name = str(getattr(module, "NAME", path.stem))
    if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
        raise ExperimentError(f"{path.name}: NAME {name!r} may use only letters, digits, '.', '_' and '-'")
    return Experiment(path, name, str(getattr(module, "DESCRIPTION", "")), bool(getattr(module, "PAID_CALLS", True)),
                      module)
