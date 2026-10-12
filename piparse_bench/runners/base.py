"""Abstract runner interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import RunConfig, RunResult


class Runner(ABC):
    @abstractmethod
    def execute(self, config: RunConfig) -> RunResult:
        """Execute one run, return metrics."""
        ...

    @abstractmethod
    def validate(self, config: RunConfig) -> list[str]:
        """Return list of validation errors (empty = valid)."""
        ...
