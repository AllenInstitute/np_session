from __future__ import annotations

import abc
from typing import Any, Iterator

import np_logging
from typing_extensions import Protocol, runtime_checkable

logger = np_logging.getLogger(__name__)


@runtime_checkable
class State(Protocol):
    def __init__(self, id: str | int) -> None:
        ...

    @classmethod
    @abc.abstractmethod
    def connect(cls) -> None:
        """Connect to the database."""

    def __getitem__(self, key: str) -> Any:
        ...

    def __setitem__(self, key: str, value: Any) -> None:
        ...

    def __delitem__(self, key: str) -> None:
        ...

    def __iter__(self) -> Iterator[str]:
        ...

    def __len__(self) -> int:
        ...
