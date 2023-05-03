from __future__ import annotations

import contextlib
import datetime
import doctest
import pathlib
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
    >>> session = TempletonPilotSession('c:/templeton/pilot/2023-01-18_10-44-55_646318_surface-image1-left.png')
    >>> session.id
    'TempletonPilot_2023-01-18_10-44-55_646318'
    >>> session.folder
    '2023-01-18_10-44-55_646318'
    >>> session.is_ecephys
    True
    >>> session.rig.acq # hostnames reflect the computers used during the session, not necessarily the current machines
    'W10DT05516'
    >>> session.npexp_path.as_posix()
    '//allen/programs/mindscope/workgroups/templeton/TTOC/pilot recordings/2023-01-18_10-44-55_646318'
    
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
    is_ecephys_session = is_ecephys   # not dealing with habs
    project = 'TempletonPilot'
    
    datetime_format: ClassVar[str] = '%Y-%m-%d_%H-%M-%S'
    
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

        if pathlib.Path(path_or_session).exists():
            self.npexp_path = pathlib.Path(path_or_session)

    @property
    def date(self) -> datetime.date:
        return datetime.datetime.strptime(self.folder[:-7], self.datetime_format).date()

    @property
    def mouse(self) -> Mouse:
        """Mouse information from the session folder name."""
        return Mouse(int(self.folder[-6:]))

    @property
    def rig(self) -> np_config.Rig:
        """Rig information from the session folder name."""
        if self.date < datetime.date(2023, 5, 1):
            return np_config.Rig(3)
        return np_config.Rig(2)

    @property
    def id(self) -> str:
        """Same as `folder`."""
        return f'{self.project}_{self.folder}'

    @property
    def folder(self) -> str:
        """Folder name."""
        return self._folder

    @folder.setter
    def folder(self, value: str | PathLike) -> None:
        folder = self.get_folder(value)
        if folder is None:
            raise ValueError(
                f'Session folder must be in the format `templeton*/pilot*/**/[date: %Y-%m-%d_%H-%M-%S]_[6-digit mouse ID]`: {value}'
            )
        self._folder = folder

    @staticmethod
    def get_folder(path: str | pathlib.Path) -> str | None:
        session_reg_exp = R'[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}_[0-9]{6}'
        session_folders = re.findall(session_reg_exp, str(path), re.IGNORECASE)
        if session_folders and 'templeton' in str(path).lower() and 'pilot' in str(path).lower():
            return session_folders[0]

    @classmethod
    def new(
        cls,
        mouse_labtracks_id: int | str | Mouse,
    ) -> Self:
        """Create a new session folder for a mouse."""
        path = cls.storage_dirs[0] / f'{datetime.datetime.now().strftime(cls.datetime_format)}_{mouse_labtracks_id}'
        return cls(path)
    
    @property
    def npexp_path(self) -> pathlib.Path:
        with contextlib.suppress(AttributeError):
            return self._npexp_path
        for _ in self.storage_dirs:
            path = _ / self.folder
            if path.exists():
                self.npexp_path = path
                break
        return self.npexp_path

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
