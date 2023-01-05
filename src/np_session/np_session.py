from __future__ import annotations

import datetime
import doctest
import logging
import os
import pathlib
from typing import Any, Generator, Optional, Union

from typing_extensions import Literal
from backports.cached_property import cached_property

if __name__ == "__main__":
    from paths import *
    from projects import *
    from utils import *

    import lims2
    import mtrain

    import data_getters as dg
else:
    from .paths import *
    from .projects import *
    from .utils import *

    from . import lims2
    from . import mtrain

    from . import data_getters as dg

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
    >>> session.id
    '1116941914'
    >>> session.folder
    '1116941914_576323_20210721'
    >>> session.project
    'BrainTV Neuropixels Visual Behavior'
    >>> session.is_ecephys_session
    True

    Some properties are returned as objects with richer information:
    - `pathlib` objects for filesystem paths:
    >>> session.lims_path.as_posix()
    '//allen/programs/braintv/production/visualbehavior/prod0/specimen_1098595957/ecephys_session_1116941914'

    - `datetime` objects for easy date manipulation:
    >>> session.date
    datetime.date(2021, 7, 21)

    - dictionaries from lims (loaded lazily):
    >>> session.mouse['id']
    1098595953
    >>> session.mouse['full_genotype']
    'wt/wt'

    ...with a useful string representation:
    >>> str(session.mouse)
    '576323'

    """

    def __init__(self, path: PathLike):

        path = pathlib.Path(path)

        self.folder = folder(path)

        if not self.folder and is_lims_path(path):
            self.folder = folder_from_lims_id(path)

        if self.folder is None:
            raise SessionError(f"{path} does not contain a valid session folder string")

        self.id = self.folder.split("_")[0]

    @property
    def lims(self) -> dict[str, Any]:
        """
        >>> info = Session('1116941914').lims
        >>> info['stimulus_name']
        'EPHYS_1_images_H_3uL_reward'
        >>> info['operator']['login']
        'taminar'

        >>> Session('1116941914').lims
        SessionInfo('1116941914')
        >>> str(Session('1116941914').lims)
        '1116941914'

        """
        if not hasattr(self, "_lims"):
            try:
                self._lims = lims2.SessionInfo(self.id)
            except ValueError:
                self._lims = {}
        return self._lims

    @property
    def mouse(self) -> str | dict[str, Any]:
        if not hasattr(self, "_mouse"):
            try:
                self._mouse = lims2.MouseInfo(self.folder.split("_")[1])
            except ValueError:
                self._mouse = {}
        return self._mouse

    @cached_property
    def date(self) -> Union[str, datetime.date]:
        d = self.folder.split("_")[2]
        date = datetime.date(year=int(d[:4]), month=int(d[4:6]), day=int(d[6:]))
        return date

    @property
    def is_ecephys_session(self) -> Optional[bool]:
        """False if behavior session, None if unsure."""
        if not self.lims:
            return None
        return "ecephys_session" in self.lims.get("storage_directory", "")

    @property
    def npexp_path(self) -> pathlib.Path:
        """get session folder from path/str and combine with npexp root to get folder path on npexp"""
        return NPEXP_ROOT / self.folder

    @property
    def lims_path(self) -> Optional[pathlib.Path]:
        """get lims id from path/str and lookup the corresponding directory in lims"""
        if not hasattr(self, "_lims_path"):
            path: str = self.lims.get("storage_directory", "")
            if not path:
                logging.debug(
                    "lims checked successfully, but no folder uploaded for ", self.id
                )
                self._lims_path = None
            else:
                self._lims_path = pathlib.Path("/" + path)
        return self._lims_path

    @property
    def project(self) -> Optional[str]:
        return self.lims.get("project", {}).get("name", None)

    @cached_property
    def lims_data_getter(self) -> Optional[dg.lims_data_getter]:
        try:
            return dg.lims_data_getter(self.id)
        except ConnectionError:
            logging.debug("Connection to lims failed", exc_info=True)
            return None
        except:
            raise

    @property
    def data_dict(self) -> Optional[dict]:
        if not hasattr(self, "_data_dict"):
            data_getter = self.lims_data_getter
            if not data_getter:
                self.data_dict = None
            else:
                self._data_dict_orig = data_getter.data_dict  # str paths
                self._data_dict = data_getter.data_dict_pathlib  # pathlib paths
        return self._data_dict

    @property
    def mtrain(self) -> Optional[dict]:
        """Info from MTrain on the last behavior session for the mouse on the experiment day"""
        if not hasattr(self, "_mtrain"):
            if not is_connected("mtrain"):
                return None
            try:
                self.mouse.mtrain = mtrain.MTrain(self.mouse)
            except mtrain.MouseNotInMTrainError:
                self._mtrain = None
            except:
                raise
            else:
                self._mtrain = self.mouse.mtrain.last_behavior_session_on(self.date)
        return self._mtrain


def on_npexp(
    project: str | Project = None, session: Literal["ecephys", "behavior"] = "ecephys"
) -> Generator[Session, None, None]:
    """Session folders on npexp.

    Project is the common-name among the neuropixels team: 'DR', 'GLO', 'VAR', 'ILLUSION'
    (use the Project enum if unsure)
    """

    if isinstance(project, str):
        project = getattr(Project, project)

    for path in NPEXP_PATH.iterdir():

        if not path.is_dir():
            continue
        try:
            session = Session(path)
        except (SessionError, FilepathIsDirError):
            continue

        if (
            session == "ecephys" and session.is_ecephys_session == False
        ):  # None = unsure and is included
            continue
        if (
            session == "behavior" and session.is_ecephys_session
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
