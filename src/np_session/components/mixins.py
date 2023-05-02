from __future__ import annotations

import abc
import datetime
import enum
import functools
import time
from typing import Any, ClassVar, Mapping, MutableMapping, Optional, TypeVar

import np_logging
from backports.cached_property import cached_property
from typing_extensions import Literal, Protocol, runtime_checkable

from np_session.databases import State
from np_session.databases.lims2 import (LIMS2MouseInfo, LIMS2ProjectInfo,
                                        LIMS2UserInfo)
from np_session.databases.mtrain import MTrain

logger = np_logging.getLogger(__name__)


@runtime_checkable
class WithState(Protocol):
    """Protocol for types that have a `state` attribute for persisting
    metadata. Can also be used as a mixin to provide basic state implementation.
    """
    
    id: int | str
    """Unique identifier for the object. This is used as the key for the object's state in the database."""
    
    @property
    def state(self) -> State:
        try:
            return State(self.id)
        except Exception as exc:
            logger.error("Failed to load `%r.state`: %r", self, exc)
        return {}
