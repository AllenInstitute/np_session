from __future__ import annotations

import contextlib
import datetime
import doctest
import pathlib
import warnings

import np_config
import np_logging
from typing_extensions import Self

from np_session.components.info import Mouse, Project, User
from np_session.components.paths import *
from np_session.components.platform_json import *
from np_session.session import Session
from np_session.utils import *

logger = np_logging.getLogger(__name__)


class DRPilotSession(Session):
    """Session information from any string or PathLike containing a session ID.

    Note: lims/mtrain properties may be empty or None if mouse/session isn't in db.
    Note: `is_ecephys` checks ecephys vs behavior: habs are ecephys sessions, as in lims.

    Quick access to useful properties:
    >>> session = DRPilotSession('c:/DRPilot_366122_20220824_surface-image1-left.png')
    >>> session.id
    'DRPilot_366122_20220824'
    >>> session.folder
    'DRPilot_366122_20220824'
    >>> session.is_ecephys
    True
    >>> session.rig.acq # hostnames reflect the computers used during the session, not necessarily the current machines
    'W10DT05516'

    Some properties are returned as objects with richer information:

    - `datetime` objects for easy date manipulation:
    >>> session.date
    datetime.date(2022, 8, 24)

    - dictionaries from lims (loaded lazily):
    >>> session.mouse
    Mouse(366122)
    >>> session.mouse.lims
    LIMS2MouseInfo(366122)
    >>> session.mouse.lims.id
    657428270
    >>> session.mouse.lims['full_genotype']
    'wt/wt'

    ...with a useful string representation:
    >>> str(session.mouse)
    '366122'
    >>> str(session.project)
    'DRPilot'
    >>> str(session.rig)        # see np_config.Rig
    'NP.3'
    """

    is_ecephys = True   # not dealing with habs
    project = 'DRPilot'

    storage_dirs: ClassVar[tuple[pathlib.Path, ...]] = tuple(
        pathlib.Path(_)
        for _ in (
            '//10.128.50.140/Data2',
            '//allen/programs/mindscope/workgroups/dynamicrouting/PilotEphys/Task 2 pilot',
            '//allen/programs/mindscope/workgroups/np-exp/PilotEphys/Task 2 pilot',
        )
    )
    """Various directories where DRpilot sessions are stored - use `npexp_path`
    to get the session folder that exists."""

    def __init__(self, path_or_session: PathLike) -> None:
        super().__init__(path_or_session)

        if pathlib.Path(path_or_session).exists():
            self.npexp_path = pathlib.Path(path_or_session)

    @property
    def rig(self) -> np_config.Rig:
        """Rig information from the session folder name."""
        if self.date < datetime.date(2023, 5, 1):
            return np_config.Rig(3)
        return np_config.Rig(2)

    @property
    def id(self) -> str:
        """Same as `folder`."""
        return self.folder

    @property
    def folder(self) -> str:
        """Folder name, e.g. `DRpilot_[labtracks ID]_[8-digit date]`."""
        return self._folder

    @folder.setter
    def folder(self, value: str | PathLike) -> None:
        folder = self.get_folder(value)
        if folder is None:
            raise ValueError(
                f'Session folder must be in the format `DRpilot_[6-digit mouse ID]_[8-digit date str]`: {value}'
            )
        self._folder = folder

    @staticmethod
    def get_folder(path: str | pathlib.Path) -> str | None:
        """Extract [DRpilot_[6-digit mouse ID]_[8-digit date str] from a string or
        path.
        """
        # from filesystem
        session_reg_exp = R'DRpilot_[0-9]{6}_[0-9]{8}'
        session_folders = re.findall(session_reg_exp, str(path), re.IGNORECASE)
        if session_folders:
            return session_folders[0]

    @classmethod
    def new(
        cls,
        mouse_labtracks_id: int | str | Mouse,
        user: Optional[str | User] = None,
        *args,
        **kwargs,
    ) -> Self:
        """Create a new session folder for a mouse."""
        path = cls.storage_dirs[0] / f'DRpilot_{mouse_labtracks_id}_{datetime.date.today().strftime("%Y%m%d")}'
        path.mkdir(parents=True, exist_ok=True)
        session = cls(path)
        if user:
            session._user = user
        session._mouse = Mouse(mouse_labtracks_id)
        return cls(path, *args, **kwargs)
    
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
            "LIMS info not available: LIMS sessions weren't created for for DRPilot experiments."
        )
        return {}


if __name__ == '__main__':
    doctest.testmod(verbose=True)
    optionflags = (
        doctest.ELLIPSIS,
        doctest.NORMALIZE_WHITESPACE,
        doctest.IGNORE_EXCEPTION_DETAIL,
    )
