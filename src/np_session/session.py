from __future__ import annotations

import contextlib
import datetime
import doctest
import functools
import os
import pathlib
from typing import Any, Generator, Union

from backports.cached_property import cached_property

import np_config
import np_logging
from typing_extensions import Literal

from np_session.components.info import Mouse, Project, Projects, User
from np_session.components.paths import *
from np_session.databases import data_getters as dg
from np_session.databases import lims2 as lims
from np_session.databases import mtrain
from np_session.databases import State
from np_session.utils import *

logger = np_logging.getLogger(__name__)

PathLike = Union[str, bytes, os.PathLike, pathlib.Path]
# https://peps.python.org/pep-0519/#provide-specific-type-hinting-support
# PathLike inputs are converted to pathlib.Path objects for os-agnostic filesystem operations.
# os.fsdecode(path: PathLike) is used where only a string is required.


class SessionError(ValueError):
    """Raised when a session folder string ([lims-id]_[mouse-id]_[date]) can't be found in a
    filepath"""

    pass


class FilepathIsDirError(ValueError):
    """Raised when a directory is specified but a filepath is required"""

    pass


class Session:
    """Session information from any string or PathLike containing a lims session ID.

    Note: lims/mtrain properties may be empty or None if mouse/session isn't in db.

    Quick access to useful properties:
    >>> session = Session('c:/1116941914_surface-image1-left.png')
    >>> session.lims.id
    1116941914
    >>> session.folder
    '1116941914_576323_20210721'
    >>> session.project.lims.id
    714916854
    >>> session.is_ecephys_session
    True
    >>> session.rig.acq # hostnames reflect the computers used during the session, not necessarily the current machines
    'W10DT05515'

    Some properties are returned as objects with richer information:
    - `pathlib` objects for filesystem paths:
    >>> session.lims_path.as_posix()
    '//allen/programs/braintv/production/visualbehavior/prod0/specimen_1098595957/ecephys_session_1116941914'
    >>> session.data_dict['storage_directory'].as_posix()
    '//allen/programs/braintv/production/visualbehavior/prod0/specimen_1098595957/ecephys_session_1116941914'


    - `datetime` objects for easy date manipulation:
    >>> session.date
    datetime.date(2021, 7, 21)
    
    - dictionaries from lims (loaded lazily):
    >>> session.mouse
    Mouse(576323)
    >>> session.mouse.lims
    LIMS2MouseInfo(576323)
    >>> session.mouse.lims.id
    1098595957
    >>> session.mouse.lims['full_genotype']
    'wt/wt'

    ...with a useful string representation:
    >>> str(session.mouse)
    '576323'
    >>> str(session.project)
    'NeuropixelVisualBehavior'
    >>> str(session.rig)        # see np_config.Rig
    'NP.0'
    """
    def __lt__(self, other: Session) -> bool:
        if not hasattr(other, 'date'):
            return NotImplemented
        return self.date < other.date

    def __str__(self) -> str:
        return self.folder
    
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.folder!r})'

    def __init__(self, path_or_session: PathLike | int | LIMS2SessionInfo):
        
        path_or_session = str(path_or_session)
        
        path_or_session = pathlib.Path(path_or_session)
        
        np_folder = folder(path_or_session)

        if not np_folder:
            np_folder = folder_from_lims_id(path_or_session)

        if np_folder is None:
            raise SessionError(f"{path_or_session} does not contain a valid lims session id or session folder string")

        self.folder = np_folder
        self.id = int(self.folder.split("_")[0])
        
        if isinstance(path_or_session, LIMS2SessionInfo):
            self._lims = path_or_session

    @property
    def lims(self) -> lims.LIMS2SessionInfo | dict:
        """
        >>> info = Session(1116941914).lims
        >>> info['stimulus_name']
        'EPHYS_1_images_H_3uL_reward'
        >>> info['operator']['login']
        'taminar'

        >>> Session(1116941914).lims
        LIMS2SessionInfo(1116941914)
        >>> str(Session(1116941914).lims)
        '1116941914'
        """
        if not hasattr(self, "_lims"):
            try:
                self._lims = lims.LIMS2SessionInfo(self.id)
            except ValueError:
                self._lims = {}
        return self._lims

    @property
    def mouse(self) -> Mouse:
        if not hasattr(self, "_mouse"):
            self._mouse = Mouse(self.folder.split("_")[1])
        return self._mouse
    
    @property
    def user(self) -> User | None:
        if not hasattr(self, "_user"):
            lims_user_id = self.lims.get('operator', {}).get('login', '')
            if lims_user_id:
                self._user = User(lims_user_id)
            else:
                self._user = None
        return self._user
    
    @cached_property
    def date(self) -> datetime.date:
        d = self.folder.split("_")[2]
        date = datetime.date(year=int(d[:4]), month=int(d[4:6]), day=int(d[6:]))
        return date

    @property
    def rig(self) -> np_config.Rig | None:
        "Rig object with computer info and paths, can also be used as a string."
        if not hasattr(self, "_rig"):
            self._rig = None
            while not self.rig:
                
                # try from current rig first
                with contextlib.suppress(ValueError):
                    self.rig = np_config.Rig()
                    continue
                    
                # TODO try from platform json
                
                # try from lims 
                rig_id: str | None = self.data_dict.get('rig')
                if rig_id:
                    self.rig = np_config.Rig(rig_id)
                    continue
                
                break
        return self._rig
    
    @rig.setter
    def rig(self, value: np_config.Rig) -> None:
        if not isinstance(value, np_config.Rig):
            raise TypeError(f'Expected `rig` to be an instance of `np_config.Rig`, not {type(value)}')
        self._rig = value
        self.update_hostnames_for_replaced_computers()

    def update_hostnames_for_replaced_computers(self) -> None:
        if not self._rig:
            return
        for comp in ("sync", "stim", "mon", "acq"):
            replaced = old_hostname(f"{self._rig.id}-{comp.capitalize()}", self.date)
            if replaced:
                setattr(self._rig, f"_{comp}", replaced)

    @property
    def is_ecephys_session(self) -> bool | None:
        """False if behavior session, None if unsure."""
        if not self.lims:
            return None
        return "ecephys_session" in self.lims.get("storage_directory", "")

    @property
    def npexp_path(self) -> pathlib.Path:
        """np-exp root / folder (may not exist)"""
        return NPEXP_ROOT / self.folder

    @property
    def lims_path(self) -> pathlib.Path | None:
        """Corresponding directory in lims, if one can be found"""
        if not hasattr(self, "_lims_path"):
            path: str = self.lims.get("storage_directory", "")
            if not path:
                logger.debug(
                    "lims checked successfully, but no folder uploaded for ", self.id
                )
                self._lims_path = None
            else:
                self._lims_path = pathlib.Path("/" + path)
        return self._lims_path
    
    @property
    def z_path(self) -> pathlib.Path:
        "Path in Sync neuropixels_data (aka Z:) (may not exist)) "
        return np_config.local_to_unc(self.rig.sync, NEUROPIXELS_DATA_RELATIVE_PATH) / self.folder
    
    @property
    def qc_path(self) -> pathlib.Path:
        "Expected default path, or alternative if one exists - see `qc_paths` for all available"
        return self.qc_paths[0] if self.qc_paths else QC_PATHS[0] / self.folder
    
    @cached_property
    def qc_paths(self) -> list[pathlib.Path]:
        "Any QC folders that exist"
        return [path / self.folder for path in QC_PATHS if (path / self.folder).exists()]    
    
    @property
    def project(self) -> Project | None:
        if not hasattr(self, "_project"):
            lims_project_name = self.lims.get('project', {}).get('code', '')
            if lims_project_name:
                self._project = Project(lims_project_name)
            else:
                self._project = None
        return self._project
    
    @cached_property
    def lims_data_getter(self) -> dg.data_getter | None:
        try:
            return dg.lims_data_getter(self.id)
        except ConnectionError:
            logger.debug("Connection to lims failed", exc_info=True)
            return None
        except:
            raise

    @property
    def data_dict(self) -> dict:
        if not hasattr(self, "_data_dict"):
            data_getter = self.lims_data_getter
            if not data_getter:
                self._data_dict = {}
            else:
                self._data_dict_orig = data_getter.data_dict  # str paths
                self._data_dict = data_getter.data_dict_pathlib  # pathlib paths
        return self._data_dict

    @property
    def mtrain(self) -> mtrain.MTrain | dict:
        """Info from MTrain on the last behavior session for the mouse on the experiment day"""
        if not hasattr(self, "_mtrain"):
            if not is_connected("mtrain"):
                return {}
            try:
                _ = self.mouse.mtrain
            except mtrain.MouseNotInMTrainError:
                self._mtrain = {}
            except:
                raise
            else:
                self._mtrain = self.mouse.mtrain.last_behavior_session_on(self.date)
        return self._mtrain

    @property
    def foraging_id(self) -> str | None:
        """Foraging ID from MTrain (if an MTrain session is found)."""
        return self.mtrain.get("id", None)

    @cached_property
    def state(self) -> State:
        return State(self.id)
        
def generate_session(
    mouse: str | int | Mouse,
    user: str | User,
    session_type: Literal["ecephys", "hab"] = "ecephys",
) -> Session:
    if not isinstance(mouse, Mouse):
        mouse = Mouse(mouse)
    if not isinstance(user, User):
        user = User(user)
    if session_type == "ecephys":
        lims_session = lims.generate_ecephys_session(mouse=mouse.lims, user=user.lims)
    elif session_type == "hab":
        lims_session = lims.generate_hab_session(mouse=mouse.lims, user=user.lims)
    session = Session(lims_session)
    # assign instances with data already fetched from lims:
    session._mouse = mouse  
    session._user = user
    return session

def sessions(
    path = NPEXP_ROOT,
    project: str | Projects = None,
    session_type: Literal["ecephys", "behavior"] = "ecephys",
) -> Generator[Session, None, None]:
    """Recursively find Session folders in a directory.

    Project is the common-name among the neuropixels team: 'DR', 'GLO', 'VAR', 'ILLUSION'
    (use the Project enum if unsure)
    """

    if isinstance(project, str):
        project = getattr(Projects, project)

    for path in NPEXP_PATH.iterdir():

        if not path.is_dir():
            continue
        try:
            session = Session(path)
        except (SessionError, FilepathIsDirError):
            continue

        if (
            session_type == "ecephys" and session.is_ecephys_session == False
        ):  # None = unsure and is included
            continue
        if (
            session_type == "behavior" and session.is_ecephys_session
        ):  # None = unsure and is included
            continue

        if project and session.project not in project.value:
            continue

        yield session


if __name__ == "__main__":

    if is_connected("lims2"):
        doctest.testmod()
        # optionflags=(doctest.ELLIPSIS, doctest.NORMALIZE_WHITESPACE, doctest.IGNORE_EXCEPTION_DETAIL)
    else:
        print("LIMS not connected - skipping doctests")
