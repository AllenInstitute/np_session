from __future__ import annotations

import contextlib
import dataclasses
import datetime
import doctest
import pathlib
from typing import NamedTuple
import warnings

import np_config
import np_logging
from typing_extensions import Self

from np_session.components.info import Project, User, Mouse
from np_session.components.paths import *
from np_session.components.platform_json import *
from np_session.utils import *
from np_session.session import Session

logger = np_logging.getLogger(__name__)


class TempletonPilotSession(Session):
    """Session information from any string or PathLike containing a session ID.

    Note: lims/mtrain properties may be empty or None if mouse/session isn't in db.
    Note: `is_ecephys` checks ecephys vs behavior: habs are ecephys sessions, as in lims.

    Quick access to useful properties:
    >>> session = TempletonPilotSession("//allen/programs/mindscope/workgroups/templeton/TTOC/pilot recordings/2023-01-18_10-44-55_646318")
    >>> session.id
    '2023-01-18_10-44-55_646318'
    >>> session.folder
    'TempletonPilot_646318_20230118_104455'
    >>> session.is_ecephys
    True
    >>> session.rig.acq # hostnames reflect the computers used during the session, not necessarily the current machines
    'W10DT05516'
    >>> session.npexp_path.as_posix()
    '//allen/programs/mindscope/workgroups/templeton/TTOC/pilot recordings/2023-01-18_10-44-55_646318'
    >>> session1 = TempletonPilotSession("2023-01-18_10-44-55_646318")
    >>> session2 = TempletonPilotSession("TempletonPilot_646318_20230118_104455")
    >>> session == session1 == session2
    True
    
    Some properties are returned as objects with richer information:

    - `datetime` objects for easy date manipulation:
    >>> session.date
    datetime.date(2023, 1, 18)

    - dictionaries from lims (loaded lazily):
    >>> session.mouse
    Mouse(646318)
    >>> session.mouse.lims
    LIMS2MouseInfo(646318)
    >>> session.mouse.lims.id
    1220603879
    >>> session.mouse.lims['full_genotype']
    'wt/wt'

    ...with a useful string representation:
    >>> str(session.mouse)
    '646318'
    >>> str(session.project)
    'TempletonPilot'
    >>> str(session.rig)        # see np_config.Rig
    'NP.3'
    """

    is_ecephys = True   # not dealing with habs
    project = 'TempletonPilot'
    
    ephys_date_format: ClassVar[str] = '%Y-%m-%d'
    ephys_time_format: ClassVar[str] = '%H-%M-%S'
    session_date_format: ClassVar[str] = '%Y%m%d'
    session_time_format: ClassVar[str] = '%H%M%S'
    
    session_reg_exp = re.compile(project + R'_[0-9]{6}_[0-9]{8}_[0-9]{6}')
    ephys_reg_exp = re.compile(R'[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}_[0-9]{6}')
    
    storage_dirs: ClassVar[tuple[pathlib.Path, ...]] = tuple(
        pathlib.Path(_)
        for _ in (
            "//allen/programs/mindscope/workgroups/templeton/TTOC/pilot recordings",
        )
    )
    """Various directories where Templeton pilot sessions are stored - use `npexp_path`
    to get the session folder that exists."""

    def __init__(self, path_or_session: PathLike) -> None:
        super().__init__(path_or_session)

        self.npexp_path = self.get_extant_path(path_or_session) or self.get_extant_path(self.folder)
    
    @classmethod
    def get_extant_path(cls, path: str | PathLike) -> pathlib.Path | None:
        if pathlib.Path(path).exists():
            return pathlib.Path(path)
        for d in cls.storage_dirs:
            for s in (
                pathlib.Path(path).name,
                cls.info_to_ephys_folder(cls.info_from_path(path)),
                cls.info_to_session_folder(cls.info_from_path(path)),
            ):
                if (d / s).exists():
                    return d / s
    
    class InfoFromPath(NamedTuple):
        mouse: str
        date: datetime.date
        time: datetime.time

    @classmethod
    def info_from_path(cls, path: str | PathLike) -> InfoFromPath | None:
        """Parse a string or path to get mouse, date, time, etc."""
        path = str(path)
        if cls.session_reg_exp.search(path):
            _, mouse, date, time = cls.session_reg_exp.search(path).group().split('_')
            return cls.InfoFromPath(mouse, datetime.datetime.strptime(date, cls.session_date_format).date(), datetime.datetime.strptime(time, cls.session_time_format).time())
        if cls.ephys_reg_exp.search(path):
            date, time, mouse = cls.ephys_reg_exp.search(path).group().split('_')
            return cls.InfoFromPath(mouse, datetime.datetime.strptime(date, cls.ephys_date_format).date(), datetime.datetime.strptime(time, cls.ephys_time_format).time())
    
    @property
    def info(self) -> InfoFromPath:
        return self.info_from_path(self.npexp_path)
    
    @property
    def date(self) -> datetime.date:
        return self.info.date

    @property
    def time(self) -> datetime.time:
        return self.info.time

    @property
    def mouse(self) -> Mouse:
        """Mouse information from the session folder name."""
        return Mouse(self.info.mouse)

    @property
    def rig(self) -> np_config.Rig:
        """Rig information from the session folder name."""
        if self.date < datetime.date(2023, 5, 1):
            return np_config.Rig(3)
        return np_config.Rig(2)

    @property
    def id(self) -> str:
        return self.ephys_folder

    @property
    def folder(self) -> str:
        """Folder name."""
        return self._folder
    
    @property
    def session_folder(self) -> str:
        """A made-up folder name that matches other session folders."""
        return self.info_to_session_folder(self.info)
    @property
    def ephys_folder(self) -> str:
        """Original format of session folders (default from Open Ephys)."""
        return self.info_to_ephys_folder(self.info)

    @classmethod
    def info_to_session_folder(cls, info: InfoFromPath) -> str:
        return f'{cls.project}_{info.mouse}_{info.date.strftime(cls.session_date_format)}_{info.time.strftime(cls.session_time_format)}'

    @classmethod
    def info_to_ephys_folder(cls, info: InfoFromPath) -> str:
        return f'{info.date.strftime(cls.ephys_date_format)}_{info.time.strftime(cls.ephys_time_format)}_{info.mouse}'

    @folder.setter
    def folder(self, value: str | PathLike) -> None:
        folder = self.get_folder(value)
        if folder is None:
            raise ValueError(
                f'Session folder must be in the format `[datetime: %Y-%m-%d_%H-%M-%S]_[6-digit mouse ID]`: {value}'
            )
        self._folder = folder

    @classmethod
    def get_folder(cls, path: str | pathlib.Path) -> str | None:
        info = cls.info_from_path(path)
        if info is None:
            return None
        if cls.get_extant_path(path):
            return cls.info_to_session_folder(info)
            
    @classmethod
    def new(
        cls,
        mouse_labtracks_id: int | str | Mouse,
        user: Optional[str | User] = None,
        *args,
        **kwargs,
    ) -> Self:
        """Create a new session folder for a mouse."""
        path = cls.storage_dirs[0] / f'{datetime.datetime.now().strftime(f"{cls.ephys_date_format}_{cls.ephys_time_format}")}_{mouse_labtracks_id}'
        path.mkdir(parents=True, exist_ok=True)
        session = cls(path)
        if user:
            session._user = user
        session._mouse = Mouse(mouse_labtracks_id)
        return session
    
    @property
    def npexp_path(self) -> pathlib.Path:
        with contextlib.suppress(AttributeError):
            return self._npexp_path
        
        raise FileNotFoundError(f'Could not find {self.folder} of {self.id} in {self.storage_dirs}')

    @npexp_path.setter
    def npexp_path(self, value: pathlib.Path) -> None:
        logger.debug('Setting %s npexp_path to %s', self, value)
        self._npexp_path = value

    @property
    def lims(self) -> dict:
        warnings.warn(
            "LIMS info not available: LIMS sessions weren't created for for TempletonPilot experiments."
        )
        return {}


if __name__ == '__main__':
    doctest.testmod(verbose=True)
    optionflags = (
        doctest.ELLIPSIS,
        doctest.NORMALIZE_WHITESPACE,
        doctest.IGNORE_EXCEPTION_DETAIL,
    )
