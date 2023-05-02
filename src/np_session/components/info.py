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

class InfoBaseClass(abc.ABC):
    "Store details for an object from various databases. The commonly-used format of its name, e.g. '366122' for a mouse ID, can be obtained by converting to str()."

    id: int | str
    "Commonly-used format of the object's value among the neuropixels team e.g. for a mouse -> the labtracks ID (366122)."

    def __str__(self) -> str:
        return str(self.id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.id!r})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (int, str, InfoBaseClass)):
            return NotImplemented
        if isinstance(other, Mouse):
            return self.id == other.id
        return str(self) == str(other) or str(self.id) == str(other)
    
    def __hash__(self) -> int:
        return hash(self.id) ^ hash(self.__class__.__name__)
    

@runtime_checkable
class WithLims(Protocol):
    """Potocol for types that have a `lims` attribute for accessing relevant
    lims2 data.
    """
    
    id: int | str
    """Lims2 ID for the object."""
    
    lims: dict = {}
    """lims2 data as a dict (may be empty)."""
    
@runtime_checkable
class WithState(Protocol):
    """Protocol for types that have a `state` attribute for persisting
    metadata. Can also be used as a mixin to provide basic state implementation.
    """
    
    id: int | str
    """Unique identifier for the object. This is used as the key for the object's state in the database."""
    
    @property
    def state(self) -> MutableMapping[str, Any]:
        try:
            return State(self.id)
        except Exception as exc:
            logger.error("Failed to load `%r.state`: %r", self, exc)
        return {}


class Mouse(WithLims, WithState, InfoBaseClass):
    def __init__(self, labtracks_mouse_id: str | int):
        self.id = int(labtracks_mouse_id)

    @property
    def lims(self) -> LIMS2MouseInfo | dict:
        "Lims info for the mouse."
        if not hasattr(self, "_lims"):
            try:
                self._lims = LIMS2MouseInfo(self.id)
            except ValueError:
                self._lims = {}
        return self._lims

    @cached_property
    def mtrain(self) -> MTrain:
        "Lims info for the mouse."
        return MTrain(self.id)

    @property
    def project(self) -> str | None:
        "Project associated with the mouse."
        return self.lims.project_name if self.lims else None
    
    
class User(WithState, InfoBaseClass):
    def __init__(self, lims_user_id: str):
        self.id = str(lims_user_id)

    @cached_property
    def lims(self) -> LIMS2UserInfo | dict:
        "Lims info for the user."
        if not hasattr(self, "_lims"):
            try:
                self._lims = LIMS2UserInfo(self.id)
            except ValueError:
                self._lims = {}
        return self._lims

# class ProjectsEnum(abc.ABCMeta, enum.EnumMeta, type):
#     pass

class Projects(enum.Enum):
    "All specific project names (used on lims) associated with each umbrella project."

    VAR = (
        "VariabilitySpontaneous",
        "VariabilityAim1",
    )
    GLO = ("OpenScopeGlobalLocalOddball",)
    ILLUSION = ("OpenScopeIllusion",)
    DRDG = (
        "DynamicRoutingSurgicalDevelopment",
        "DynamicRoutingDynamicGating",
        "DynamicRoutingTask1Production",
    )
    """Dynamic Gating subset of DR."""
    DRPILOT = ("DRPilot",)
    DR = DRDG + DRPILOT
    """All Dynamic Routing, including Dynamic Gating & DRpilot."""
    VB = ("NeuropixelVisualBehavior",)
    TTN = ('TaskTrainedNetworksNeuropixel',)
    NP = ('NeuropixelPlatformDevelopment',)
    
    @property
    def id(self) -> str:
        return str(self.name)
    
    @property
    def state(self) -> MutableMapping[str, Any]:
        try:
            return State(self.id)
        except Exception as exc:
            logger.error("Failed to load `%r.state`: %r", self, exc)
        return {}
    
    @property
    def latest_session(self, session_type: Literal['ephys', 'hab', 'behavior'] = 'ephys') -> int | None:
        "Lims session id for latest session for all child projects."
        for _ in self.value:
            session_id = Project(_).state.get(f'latest_{session_type}')
            if session_id:
                return session_id
        return None
    
    @latest_session.setter
    def latest_session(self, session_id: int) -> None:
        for _ in self.value:
            Project(_).state[f'latest_session'] = session_id
    
    latest_ephys = functools.partialmethod(latest_session, 'ephys')
    latest_hab = functools.partialmethod(latest_session, 'hab')
    latest_behavior = functools.partialmethod(latest_session, 'behavior')

    
    
class Project(WithState, InfoBaseClass):
    
    def __init__(self, lims_project_name: str):
        self.id = str(lims_project_name)

    @cached_property
    def lims(self) -> LIMS2ProjectInfo:
        "Lims info for the project."
        return LIMS2ProjectInfo(self.id)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Projects):
            return self.id in other.value
        return super().__eq__(other)

    @property
    def parent(self) -> Projects | None:
        "Parent project if it exists."
        for _ in Projects:
            if self.id in _.value:
                return _
        return None

    
class Dye(WithState):
    """Info about a DiI or DiO dye.
    
    >>> dye = Dye(1)
    >>> dye.description
    'CM-DiI 100%'
    >>> dye.previous_uses = 0
    >>> dye.record_first_use(time.time())
    >>> dye.increment_uses()
    >>> dye.previous_uses
    1
    """
    
    descriptions: ClassVar = ('CM-DiI 100%', 'DiO')
    
    def __init__(self, dye_id: int) -> None:
        self.id = int(dye_id)
        
    @property
    def previous_uses(self) -> int:
        return self.state.setdefault('previous_uses', 0)
    
    @previous_uses.setter
    def previous_uses(self, value: int) -> None:
        self.state['previous_uses'] = int(value)
    
    @property
    def description(self) -> Literal['CM-DiI` 100%', 'DiO']:
        return self.state.setdefault('description', self.descriptions[0])
    
    @description.setter
    def description(self, value: Literal['CM-DiI 100%', 'DiO']) -> None:
        self.state['description'] = value
    
    @property
    def first_use(self) -> datetime.datetime | None:
        first_use: float | None = self.state.get('first_use')
        return datetime.datetime.fromtimestamp(first_use) if first_use else None
    
    def record_first_use(self, timestamp: Optional[datetime.datetime | float] = None) -> None:
        """Record the time this dye was first used.
        
        Supply a `time.time()`, else the current time will be recorded.
        """
        timestamp = timestamp.timestamp() if isinstance(timestamp, datetime.datetime) else timestamp
        self.state['first_use'] = time.time() if timestamp is None else timestamp
    
    def increment_uses(self):
        """Increment the number of times this dye has been used."""
        previous_uses = self.previous_uses
        if previous_uses == 0:
            self.record_first_use()
        self.previous_uses = self.previous_uses + 1
        
        
if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=True, optionflags=doctest.IGNORE_EXCEPTION_DETAIL)