from __future__ import annotations

import abc
import contextlib
import copy
import csv
import datetime
import functools
import json
import pathlib
import re
import tempfile
import time
from typing import (Any, ClassVar, Dict, ForwardRef, Generator, Iterable, List, Mapping,
                    Optional, Sequence, Union)

import np_config
import np_logging
import pydantic
from typing_extensions import Literal

logger = np_logging.getLogger(__name__)



class PlatformJsonDateTime(datetime.datetime):
    """

    """

    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield cls.validate
        
    @classmethod
    def __modify_schema__(cls, field_schema):
        # __modify_schema__ should mutate the dict it receives in place,
        # the returned value will be ignored
        field_schema.update(
            pattern='[0-9]{14}',
            # some example postcodes
            examples=['20220414134738'],
        )
    # def __init__(self, v) -> None:
    #     super().__init__(*self.str2components(np_config.normalize_time(v)))
        
    @classmethod
    def validate(cls, v):
        if not v:
            return None 
        if not isinstance(v, str) and len(v) != 14:
            raise TypeError('14-digit string required')
        return cls(*cls.str2components(np_config.normalize_time(v)))

    @staticmethod
    def str2components(v: str) -> tuple[int, int, int, int, int, int, int]:
        return (int(v[:4]), *(int(v[_:_+2]) for _ in range(4, 14, 2)))
    
    def __str__(self):
        return np_config.normalize_time(self)
    
    def isoformat(self, *args, **kwargs) -> str:
        return str(self)
    
class PlatformJson(pydantic.BaseModel):
    """Writes D1 platform json for lims upload. Just requires a path (dir or dir+filename)."""

    # ------------------------------------------------------------------------------------- #
    # required kwargs on init (any property without a default value or leading underscore)
    
    path: pathlib.Path
    "Typically the storage directory for the session. Will be modified on assignment."
    
    # ------------------------------------------------------------------------------------- #
    
    file_sync: bool = True
    
    @contextlib.contextmanager
    def sync_disabled(self)  -> Generator[None, None, None]:
        "Context manager to temporarily disable writing to file when a property is updated."
        self.file_sync = False
        yield
        self.file_sync = True
        
    class Config:
        validate_assignment = True # coerce types on assignment
        extra = 'allow' # 'forbid' = properties must be defined in the model
        fields = {'path': {'exclude': True}, 'file_sync': {'exclude': True}}
        arbitrary_types_allowed = True
        
    suffix: ClassVar[str] = "_platformD1.json"
        
    def __init__(self, path: Union[str, pathlib.Path]) -> None:
        super().__init__(path=path)
        if self.path.exists():
            logger.debug("Loading from existing %s", self.path.name)
        else:
            logger.debug("Creating new %s", self.path.name)
            self.platform_json_creation_time = np_config.normalize_time(time.time())
        self.load_from_existing()
        
    def __str__(self):
        return self.path.as_posix()
    
    def load_from_existing(self) -> None:
        "Update empty fields with non-empty fields from file."
        with self.sync_disabled():
            if not self.path.exists():
                return
            contents = json.loads(self.path.read_text() or "{}")
            for k, v in contents.items():
                if not v:
                    continue
                if isinstance(v, (dict, list)) and not all(_ for _ in v):
                    continue
                setattr(self, k, v)
    
    def __setattr__(self, name, value):
        
        # if field is in non-validated list, just set it 
        if name in (k for k, v in self.Config.fields.items() if v.get('exclude')):
            return super().__setattr__(name, value)
        
        if self.file_sync:
            # fetch fields from disk before writing, in case another process updated the
            # file since we last read it
            self.load_from_existing()
        
        if getattr(self, name, None) == value:
            return
        
        _ = super().__setattr__(name, value)
        
        logger.debug("Updated %s.%s = %s", self.path.name, name, value)
            
        if self.file_sync:
            self.write()
            
        return _
    
    def write(self): 
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch()
        with self.sync_disabled():
            self.platform_json_save_time = np_config.normalize_time(time.time())
        self.path.write_text(self.json(indent=4))
        logger.debug("%s wrote to %s", self.__class__.__name__, self.path.as_posix())
    
    @pydantic.validator("path", pre=True)
    def normalize_path(cls, v: Union[str, pathlib.Path]) -> pathlib.Path:
        return np_config.normalize_path(v)
    
    @pydantic.validator("path")
    def add_filename_to_path(cls, v: pathlib.Path) -> pathlib.Path:
        name = cls.append_suffix_to_filename(v.name)
        return v / name if v.is_dir() else v.with_name(name)
    
    @classmethod
    def append_suffix_to_filename(cls, v: str) -> str:
        v = v.split('.json')[0].split('platform')[0].rstrip('_')                                       
        v += cls.suffix
        return v

    _foraging_id_re: ClassVar[str] = (
        r"([0-9,a-f]{8}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{12})" \
        r"|([0-9,a-f]{8})"
    )
    
    # auto-generated / ignored ------------------------------------------------------------- #
    platform_json_save_time: Union[PlatformJsonDateTime, str] = ''
    "Updated on write."
    rig_id: Optional[str] = np_config.Rig().id if np_config.RIG_IDX else None
    wfl_version: float = 0
    platform_json_creation_time: Union[PlatformJsonDateTime, str] = ""
    # = pydantic.Field(
    #     default_factory=lambda: np_config.normalize_time(time.time()),
    #     validate=PlatformJsonDateTime.validate,
    # )
    
    # pre-experiment
    # ---------------------------------------------------------------------- #
    workflow_start_time: Union[PlatformJsonDateTime, str] = ""
    operatorID: Optional[str] = ""
    sessionID: Optional[Union[str, int]] = ""
    mouseID: Optional[Union[str, int]] = ""
    project: Optional[str] = ""
    hab: Optional[bool] = None
    
    DiINotes: Dict[str, Union[str, int]] = dict(
        EndTime="", StartTime="", dii_description="", times_dipped="", previous_uses="",
    )
    HardwareConfiguration: Optional[Dict] = {}
    probe_A_DiI_depth: str = ""
    probe_B_DiI_depth: str = ""
    probe_C_DiI_depth: str = ""
    probe_D_DiI_depth: str = ""
    probe_E_DiI_depth: str = ""
    probe_F_DiI_depth: str = ""
    water_calibration_heights: List[float] = [0.0]
    water_calibration_volumes: List[float] = [0.0]
    mouse_weight_pre: str = ""
    mouse_weight_pre_float: float = 0.0
    
    HeadFrameEntryTime: Union[PlatformJsonDateTime, str] = ''
    wheel_height: str = ""
    CartridgeLowerTime: Union[PlatformJsonDateTime, str] = ''
    ProbeInsertionStartTime: Union[PlatformJsonDateTime, str] = ''
    ProbeInsertionCompleteTime: Union[PlatformJsonDateTime, str] = ''
    InsertionNotes: Dict[str, Dict] = pydantic.Field(default_factory=dict)
    ExperimentStartTime: Union[PlatformJsonDateTime, str] = ''
    stimulus_name: str = ""
    "MTrain stage (?)."
    script_name: Union[pathlib.Path, str] = ""
    "Path to stimulus script."
    
    # post-experiment ---------------------------------------------------------------------- #
    ExperimentCompleteTime: Union[PlatformJsonDateTime, str] = ''
    ExperimentNotes: Dict[str, Dict[str, Any]] = dict(
        BleedingOnInsertion={}, BleedingOnRemoval={}
    )
    foraging_id: str = pydantic.Field(default="", regex=_foraging_id_re)
    foraging_id_list: List[str] = pydantic.Field(
        default_factory=lambda: [""], regex=_foraging_id_re
    )
    HeadFrameExitTime: Union[PlatformJsonDateTime, str] = ''
    mouse_weight_post: str = ""
    water_supplement: float = 0.0
    manifest_creation_time: Union[PlatformJsonDateTime, str] = ''
    workflow_complete_time: Union[PlatformJsonDateTime, str] = ''
    
    manipulator_coordinates: Dict[str, Dict[Any, Any]] = pydantic.Field(default_factory=dict)

    files: Dict[str, Dict[str, str]] = pydantic.Field(default_factory=dict)

    def update(self, field, new) -> None:
        
        self.load_from_existing()
        
        existing = getattr(self, field)
        
        if (not new and new is not False) or existing == new:
            return
        
        # now merge the new value with the existing value in the file if it's a dict
        with contextlib.suppress(TypeError, AttributeError):
            new = np_config.merge(copy.deepcopy(getattr(self, field)), new)
            
        logger.debug('Updating %s %s: %s -> %s', self.path.name, field, existing, new)
        with self.sync_disabled():
            setattr(self, field, new)
        self.write()
   
def update_from_session(pj: PlatformJson, session) -> None:
    "Updates fields in a platform json file."
    #! careful not to execute Session methods that call this platform json instance in a loop
    
    with pj.sync_disabled():
        logger.debug("Updating %s with session %s fields, with write disabled", pj.path.name, session.id)
        pj.update('operatorID', str(session.user)) # don't need to convert here, `update` will compare values with existing
        pj.update('sessionID', str(session.id))
        pj.update('mouseID', str(session.mouse.id))
        pj.update('stimulus_name', session.lims['stimulus_name'])
        if pj.script_name:
            with contextlib.suppress(Exception):
                if session.rig.stim not in pj.script_name.as_posix():
                    pj.update('script_name', np_config.local_to_unc(session.rig.stim, pj.script_name).as_posix())
        pj.update('foraging_id', session.foraging_id)
        pj.update('project', session.project.id)
        pj.update('hab', session.is_hab)
        
    pj.write()


