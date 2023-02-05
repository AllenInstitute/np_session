from __future__ import annotations

import abc
import collections
import datetime
import enum
import functools
import json
import logging
import pathlib
import re
from typing import Any, Callable, ClassVar, Type, Union

from np_session.databases.lims2 import (LIMS2MouseInfo, LIMS2ProjectInfo,
                                        LIMS2UserInfo)
from np_session.databases.mtrain import MTrain


class InfoBaseClass(abc.ABC):
    "Store details for an object from various databases. The commonly-used format of its name, e.g. '366122' for a mouse ID, can be obtained by converting to str()."

    id: int | str
    "Commonly-used format of the object's value among the neuropixels team e.g. for a mouse -> the labtracks ID (366122)."
    def __str__(self) -> str:
        return str(self.id)
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.id!r})"
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return self.id == str(other)
        if isinstance(other, int):
            return self.id == int(str(other))
        return False

class MouseInfo(InfoBaseClass):
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
    
    @functools.cached_property
    def mtrain(self) -> MTrain:
        "Lims info for the mouse."
        return MTrain(self.id)
    
    
class UserInfo(InfoBaseClass):
    def __init__(self, lims_user_id: str):
        self.id = str(lims_user_id)
        
    @functools.cached_property
    def lims(self) -> LIMS2UserInfo | dict:
        "Lims info for the user."
        if not hasattr(self, "_lims"):
            try:
                self._lims = LIMS2UserInfo(self.id)
            except ValueError:
                self._lims = {}
        return self._lims

class Projects(enum.Enum):
    "All specific project names (used on lims) associated with each umbrella project."
    
    VAR = (
        "VariabilitySpontaneous", 
        "VariabilityAim1",
        )
    GLO = (
        "OpenScopeGlobalLocalOddball",
        )
    ILLUSION = (
        "OpenScopeIllusion",
        )
    DR = (
        "DynamicRoutingSurgicalDevelopment",
        "DynamicRoutingDynamicGating",
        "DynamicRoutingTask1Production",
    )
    VB = (
        'NeuropixelVisualBehavior',
    )
    
class ProjectInfo(InfoBaseClass):
    
    def __init__(self, lims_project_name: str):
        self.id = str(lims_project_name)
        
    @functools.cached_property
    def lims(self) -> LIMS2ProjectInfo:
        "Lims info for the project."
        return LIMS2ProjectInfo(self.id)
    
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Projects):
            return self.id in other.value
        return super().__eq__(other)
    