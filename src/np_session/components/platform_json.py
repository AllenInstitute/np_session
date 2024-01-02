from __future__ import annotations

import contextlib
import copy
import datetime
import json
import pathlib
import re
import typing
import time
from typing import Any, ClassVar, Dict, Generator, List, Optional, Union, Annotated

import np_config
import np_logging
import pydantic
from pydantic_core import CoreSchema, core_schema

logger = np_logging.getLogger(__name__)


class PlatformJsonDateTime(datetime.datetime):

    @classmethod
    def validate(cls, v, *args, **kwargs):
        if not v:
            return None
        if not isinstance(v, str) and len(v) != 14:
            raise TypeError('14-digit string required')
        return cls(*cls.str2components(np_config.normalize_time(v)))

    @staticmethod
    def str2components(v: str) -> tuple[int, int, int, int, int, int, int]:
        return (int(v[:4]), *(int(v[_ : _ + 2]) for _ in range(4, 14, 2)))

    def __str__(self):
        return np_config.normalize_time(self)

    def isoformat(self, *args, **kwargs) -> str:
        return str(self)


class _PlatformJsonDateTimeAnnotation:

    @classmethod
    def __get_pydantic_core_schema__(
        cls, 
        source_type: typing.Any, 
        handler: pydantic.GetCoreSchemaHandler
    ) -> CoreSchema:
        """
        We return a pydantic_core.CoreSchema that behaves in the following ways:

        * strs will be parsed as `PlatformJsonDatetime` instances
        * `PlatformJsonDatetime` instances will be parsed as `PlatformJsonDatetime` instances without any changes
        * Nothing else will pass validation
        * Serialization will always return just a str
        """
        def validate_from_str(v: str) -> str:
            if not v:
                return None
            if not isinstance(v, str) and len(v) != 14:
                raise TypeError('14-digit string required')
            return PlatformJsonDateTime(
                *PlatformJsonDateTime.str2components(
                    np_config.normalize_time(v)
                )
            )

        from_str_schema = core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(
                    validate_from_str
                ),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=from_str_schema,
            python_schema=core_schema.union_schema(
                [
                    # check if it's an instance first before doing any further work
                    core_schema.is_instance_schema(PlatformJsonDateTime),
                    from_str_schema,
                ]
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        _core_schema: core_schema.CoreSchema,
        handler: pydantic.GetJsonSchemaHandler
    ) -> pydantic.JsonSchemaValue:
        # Use the same schema that would be used for `str`
        return handler(core_schema.str_schema())


# We now create an `Annotated` wrapper that we'll use as the annotation for fields on `BaseModel`s, etc.
PydanticPlatformJsonDateTime = Annotated[
    PlatformJsonDateTime,
    _PlatformJsonDateTimeAnnotation,
    pydantic.PlainValidator(PlatformJsonDateTime.validate),
]


class PlatformJson(pydantic.BaseModel):
    """Writes D1 platform json for lims upload. Just requires a path (dir or dir+filename)."""

    # ------------------------------------------------------------------------------------- #
    # required kwargs on init (any property without a default value or leading underscore)

    path: pathlib.Path = pydantic.Field(
        exclude=True,
    )
    'Typically the storage directory for the session. Will be modified on assignment.'

    # ------------------------------------------------------------------------------------- #

    file_sync: bool = pydantic.Field(
        default=True,
        exclude=True,
    )

    @contextlib.contextmanager
    def sync_disabled(self) -> Generator[None, None, None]:
        """Context manager to temporarily disable writing to file when a property is updated."""
        self.file_sync = False
        yield
        self.file_sync = True   

    model_config = pydantic.ConfigDict(
        validate_assignment=True,
        extra='allow',
        arbitrary_types_allowed=True,

    )

    suffix: ClassVar[str] = '_platformD1.json'

    def __init__(self, path: Union[str, pathlib.Path]) -> None:
        super().__init__(path=path)
        if self.path.exists():
            logger.debug('Loading from existing %s', self.path.name)
        else:
            logger.debug('Creating new %s', self.path.name)
            self.platform_json_creation_time = np_config.normalize_time(
                time.time()
            )
        self.load_from_existing()

    def __str__(self):
        return self.path.as_posix()

    def load_from_existing(self) -> None:
        """Update empty fields with non-empty fields from file."""
        with self.sync_disabled():
            if not self.path.exists():
                return
            contents = json.loads(self.path.read_text() or '{}')
            for k, v in contents.items():
                if not v:
                    continue
                if isinstance(v, (dict, list)) and not all(_ for _ in v):
                    continue
                setattr(self, k, v)

    def __setattr__(self, name, value):

        # if field is in non-validated list, just set it
        if name in (
            k for k, v in self.model_fields.items() if v.exclude
        ):
            return super().__setattr__(name, value)

        if self.file_sync:
            # fetch fields from disk before writing, in case another process updated the
            # file since we last read it
            self.load_from_existing()

        if getattr(self, name, None) == value:
            return

        _ = super().__setattr__(name, value)

        logger.debug('Updated %s.%s = %s', self.path.name, name, value)

        if self.file_sync:
            self.write()

        return _

    def write(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch()
        with self.sync_disabled():
            self.platform_json_save_time = np_config.normalize_time(
                time.time()
            )
        self.path.write_text(self.model_dump_json(indent=4))
        logger.debug(
            '%s wrote to %s', self.__class__.__name__, self.path.as_posix()
        )

    @pydantic.field_validator('path', mode="before")
    def normalize_path(cls, v: Union[str, pathlib.Path]) -> pathlib.Path:
        return np_config.normalize_path(v)

    @pydantic.field_validator('path', mode="after")
    def add_filename_to_path(cls, v: pathlib.Path) -> pathlib.Path:
        name = cls.append_suffix_to_filename(v.name)
        return v / name if v.is_dir() else v.with_name(name)

    @classmethod
    def append_suffix_to_filename(cls, v: str) -> str:
        v = v.split('.json')[0].split('platform')[0].rstrip('_')
        v += cls.suffix
        return v

    _foraging_id_re: ClassVar[str] = (
        r'([0-9,a-f]{8}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{12})'
        r'|([0-9,a-f]{8})'
    )

    # auto-generated / ignored ------------------------------------------------------------- #
    platform_json_save_time: Union[PydanticPlatformJsonDateTime, str] = ''
    'Updated on write.'
    rig_id: Optional[str] = np_config.Rig().id if np_config.RIG_IDX else None
    wfl_version: float = 0
    platform_json_creation_time: PydanticPlatformJsonDateTime = pydantic.Field(
        default_factory=lambda: np_config.normalize_time(time.time()),
    )

    # pre-experiment
    # ---------------------------------------------------------------------- #
    workflow_start_time: Union[PydanticPlatformJsonDateTime, str] = ''
    operatorID: Optional[str] = ''
    sessionID: Optional[Union[str, int]] = ''
    mouseID: Optional[Union[str, int]] = ''
    project: Optional[str] = ''
    hab: Optional[bool] = None

    DiINotes: Dict[str, Union[str, int]] = dict(
        EndTime='',
        StartTime='',
        dii_description='',
        times_dipped='',
        previous_uses='',
    )
    HardwareConfiguration: Optional[Dict] = {}
    probe_A_DiI_depth: str = ''
    probe_B_DiI_depth: str = ''
    probe_C_DiI_depth: str = ''
    probe_D_DiI_depth: str = ''
    probe_E_DiI_depth: str = ''
    probe_F_DiI_depth: str = ''
    water_calibration_heights: List[float] = [0.0]
    water_calibration_volumes: List[float] = [0.0]
    mouse_weight_pre: str = ''
    mouse_weight_pre_float: float = 0.0

    HeadFrameEntryTime: Union[PydanticPlatformJsonDateTime, str] = ''
    wheel_height: str = ''
    CartridgeLowerTime: Union[PydanticPlatformJsonDateTime, str] = ''
    ProbeInsertionStartTime: Union[PydanticPlatformJsonDateTime, str] = ''
    ProbeInsertionCompleteTime: Union[PydanticPlatformJsonDateTime, str] = ''
    InsertionNotes: Dict[str, Dict] = pydantic.Field(default_factory=dict)
    ExperimentStartTime: Union[PydanticPlatformJsonDateTime, str] = ''
    stimulus_name: str = ''
    'MTrain stage (?).'
    script_name: Union[pathlib.Path, str] = ''
    'Path to stimulus script.'

    # post-experiment ---------------------------------------------------------------------- #
    ExperimentCompleteTime: Union[PydanticPlatformJsonDateTime, str] = ''
    ExperimentNotes: Dict[str, Dict[str, Any]] = dict(
        BleedingOnInsertion={}, BleedingOnRemoval={}
    )
    foraging_id: str = pydantic.Field(default='', pattern=_foraging_id_re)
    foraging_id_list: List[str] = pydantic.Field(
        default_factory=lambda: [''],
    )
    # @pydantic.field_validator('foraging_id_list')
    # @classmethod
    # def ids_in_foraging_id_list_match_pattern(
    #         cls, v: List[str]) -> List[str]:
    #     for foraging_id in v:
    #         if re.match(cls._foraging_id_re, foraging_id) is None:
    #             raise ValueError(
    #                 'Id failed pattern check. id=%s, pattern=%s' % 
    #                 (foraging_id, cls._foraging_id_re, )
    #             )
    #     return v

    HeadFrameExitTime: Union[PydanticPlatformJsonDateTime, str] = ''
    mouse_weight_post: str = ''
    water_supplement: float = 0.0
    manifest_creation_time: Union[PydanticPlatformJsonDateTime, str] = ''
    workflow_complete_time: Union[PydanticPlatformJsonDateTime, str] = ''

    manipulator_coordinates: Dict[str, Dict[Any, Any]] = pydantic.Field(
        default_factory=dict
    )

    files: Dict[str, Dict[str, str]] = pydantic.Field(default_factory=dict)

    def update(self, field, new) -> None:

        self.load_from_existing()

        existing = getattr(self, field)

        if (not new and new is not False) or existing == new:
            return

        # now merge the new value with the existing value in the file if it's a dict
        with contextlib.suppress(TypeError, AttributeError):
            new = np_config.merge(copy.deepcopy(getattr(self, field)), new)

        logger.debug(
            'Updating %s %s: %s -> %s', self.path.name, field, existing, new
        )
        with self.sync_disabled():
            setattr(self, field, new)
        self.write()


def update_from_session(pj: PlatformJson, session) -> None:
    """Updates fields in a platform json file."""
    #! careful not to execute Session methods that call this platform json instance in a loop

    with pj.sync_disabled():
        logger.debug(
            'Updating %s with session %s fields, with write disabled',
            pj.path.name,
            session.id,
        )
        pj.update(
            'operatorID', str(session.user)
        )   # don't need to convert here, `update` will compare values with existing
        pj.update('sessionID', str(session.id))
        pj.update('mouseID', str(session.mouse.id))
        pj.update('stimulus_name', session.lims['stimulus_name'])
        if pj.script_name:
            with contextlib.suppress(Exception):
                if session.rig.stim not in pj.script_name.as_posix():
                    pj.update(
                        'script_name',
                        np_config.local_to_unc(
                            session.rig.stim, pj.script_name
                        ).as_posix(),
                    )
        pj.update('foraging_id', session.foraging_id)
        pj.update('project', session.project.id)
        pj.update('hab', session.is_hab)

    pj.write()
