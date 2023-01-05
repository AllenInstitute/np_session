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
    
    
class SessionFile:
    """Represents a single file belonging to a neuropixels ecephys session"""

    session = None

    def __init__(self, path: PathLike):
        """from the complete file path we can extract some information upon
        initialization"""

        path = pathlib.Path(path)

        # ensure the path is a file, not directory
        # ideally we would check the path on disk with pathlib.Path.is_file(), but that only works if the file exists
        # we also can't assume that a file that exists one moment will still exist the next
        # (threaded operations, deleting files etc) - so no 'if exists, .is_file()?'
        # we'll try using the suffix/extension first, but be aware that sorted probe folders named 'Neuropix-PXI-100.1'
        # will give a non-empty suffix here - probably safe to assume that a numeric suffix is never an actual file
        is_file = path.suffix != ""
        is_file = False if path.suffix.isdecimal() else is_file
        try:
            is_file = True if path.is_file() else is_file
            # is_file() returns false if file doesn't exist so only change it if it exists
        except:
            pass

        if not is_file:
            raise FilepathIsDirError(
                f"{self.__class__.__name__}: path must point to a file {path}"
            )
        else:
            try:
                self.path = path  # might be read-only, in the case of DVFiles
            except AttributeError:
                pass

        self.name = self.path.name

        # get the name of the folder the file lives in (which may be the same as self.root_path below)
        self.parent = self.path.parent

        # extract the session ID from anywhere in the path
        self.session = Session(self.path)
        if not self.session:
            raise SessionError(
                f"{self.__class__.__name__}: path does not contain a session ID {self.path.as_posix}"
            )

    @property
    def root_path(self) -> str:
        """root path of the file (may be the same as session_folder_path)"""
        # we expect the session_folder string to first appear in the path as
        # a child of some 'repository' of session folders (like npexp),
        # - split the path at the first session_folder match and call that folder the root
        parts = pathlib.Path(self.path).parts
        while parts:
            if self.session.folder in parts[0]:
                break
            parts = parts[1:]
        else:
            raise SessionError(
                f"{self.__class__.__name__}: session_folder not found in path {self.path.as_posix()}"
            )

        return pathlib.Path(str(self.path).split(str(parts[0]))[0])

    @property
    def session_folder_path(self) -> Optional[str]:
        """path to the session folder, if it exists"""

        # if a repository (eg npexp) contains session folders, the following location should exist:
        session_folder_path = self.root_path / self.session.folder
        if os.path.exists(session_folder_path):
            return session_folder_path
        # but it might not exist: we could have a file sitting in a folder with a flat structure:
        # assorted files from multiple sessions in a single folder (e.g. LIMS incoming),
        # or a folder which has the session_folder pattern plus extra info
        # appended, eg. _probeABC
        # in that case return the root path
        return self.root_path

    @property
    def session_relative_path(self) -> pathlib.Path:
        """filepath relative to a session folder's parent"""
        # wherever the file is, get its path relative to the parent of a
        # hypothetical session folder ie. session_id/.../filename.ext :
        session_relative_path = self.path.relative_to(self.root_path)
        if session_relative_path.parts[0] != self.session.folder:
            return pathlib.Path(self.session.folder, session_relative_path.as_posix())
        else:
            return session_relative_path

    @property
    def relative_path(self) -> pathlib.Path:
        """filepath relative to a session folder"""
        return pathlib.Path(self.session_relative_path.relative_to(self.session.folder))

    @property
    def root_relative_path(self) -> pathlib.Path:
        """Filepath relative to the first parent with session string in name.

        #!watch out: Different meaning of 'root' to 'root_path' above

        This property will be most useful when looking for files in lims ecephys_session_XX
        folders, since the 'first parent with session string in name' is often renamed in lims:
        e.g. '123456789_366122_20220618_probeA_sorted' becomes 'job-id/probe-id_probeA'
        - filepaths relative to the renamed folder should be preserved, so we should be
        able to glob for them using this property.
        """
        # TODO update root_path to use the same meaning of 'root'
        for parent in self.path.parents:
            if self.session.folder in parent.parts[-1]:
                return self.path.relative_to(parent)
        else:
            # if no parent with session string in name, we have a file with session
            # string in its filename, sitting in some unknown folder:
            return self.path.relative_to(self.parent)

    @property
    def probe_dir(self) -> str:
        # if a file lives in a probe folder (_probeA, or _probeABC) it may have the same name, size (and even checksum) as
        # another file in a corresponding folder (_probeB, or _probeDEF) - the data are identical if all the above
        # match, but it would still be preferable to keep track of these files separately -> this property indicates
        probe = RE_PROBES.search(self.path.parent.as_posix())
        if probe:
            probe_name = probe[0]
            # only possibile probe_names here are [A-F](any combination) or [0-5](single digit)
            if len(probe_name) == 1:
                if ord("0") <= ord(probe_name) <= ord("5"):
                    # convert single-digit probe numbers to letters
                    probe_name = chr(ord("A") + int(probe_name))
                    # controversial? mostly we store in probe dirs with letter, not digit, so
                    # for finding 'the same filename in a different location' (ie a backup)
                    # it probably makes sense to use the probe letter here to
                    # facilitate comparisons
                assert ord("A") <= ord(probe_name) <= ord("F"), logging.error(
                    "{} is not a valid probe name: must include a single digit [0-5], or some combination of capital letters [A-F]".format(
                        probe_name
                    )
                )
            else:
                assert all(letter in "ABCDEF" for letter in probe_name), logging.error(
                    "{} is not a valid probe name: must include a single digit [0-5], or some combination of capital letters [A-F]".format(
                        probe_name
                    )
                )
            return probe_name
        return None

    # backup paths below are only returned if they exist and are not the same as the
    # current file path (ie. if the file is not already in a backup location) -------------- #
    @property
    def npexp_path(self) -> pathlib.Path:
        """Expected path to a copy on npexp, regardless of whether or not it exists.
        """
        # for symmetry with other paths/backups add the 'cached' property, tho it's not
        # necessary
        self._npexp_path = NPEXP_ROOT / self.session_relative_path
        return self._npexp_path

    @property
    def npexp_backup(self) -> pathlib.Path:
        """Actual path to backup on npexp if it currently exists, and isn't our current
        file, and our current file isn't on lims"""
        if (
            self.npexp_path
            and self.npexp_path.exists()
            and self.npexp_path != self.path
            and self.session.lims_path not in self.path.parents
        ):
            return self.npexp_path
        return None

    @property
    def lims_path(self) -> pathlib.Path:
        """Expected path to a copy on lims, regardless of whether or not it exists.

        This property getter just prevents repeat calls to find the path.
        """
        if not hasattr(self, "_lims_path"):
            self._lims_path = self.get_lims_path()
        return self._lims_path

    @property
    def lims_backup(self) -> pathlib.Path:
        """Actual path to backup on LIMS if it currently exists"""
        if (
            self.lims_path
            and self.lims_path.exists()
            and self.lims_path.as_posix() != self.path.as_posix()
        ):
            return self.lims_path
        return None

    def get_lims_path(self) -> pathlib.Path:
        """Path to backup on Lims (which must exist for this current method to work)"""
        if not self.session.lims_path:
            return None

        # for files in lims 'ecephys_session_XXXX' folders, which aren't in 'job_id' sub-folders:
        if (self.session.lims_path / self.root_relative_path).is_file():
            return self.session.lims_path / self.root_relative_path

        # for files in 'job_id' folders we'll need to glob and take the most recent file
        # version (assuming this == highest job id)
        pattern = f"*/{self.root_relative_path.as_posix()}"
        matches = [
            m.as_posix() for m in self.session.lims_path.glob(pattern)
        ]  # convert to strings for sorting
        if not matches:  # try searching one subfolder deeper
            pattern = "*/" + pattern
        matches = [
            m.as_posix() for m in self.session.lims_path.glob(pattern)
        ]  # convert to strings for sorting
        if matches and self.probe_dir:
            matches = [m for m in matches if f"_probe{self.probe_dir}" in m]
        if not matches:
            return None
        return pathlib.Path(sorted(matches)[-1])

    @property
    def z_drive_path(self) -> pathlib.Path:
        """Expected path to a copy on 'z' drive, regardless of whether or not it exists.

        This property getter just prevents repeat calls to find the path.
        """
        if not hasattr(self, "_z_drive_path"):
            self._z_drive_path = self.get_z_drive_path()
        return self._z_drive_path

    @property
    def z_drive_backup(self) -> pathlib.Path:
        """Path to backup on 'z' drive if it currently exists, also considering the
        location of the current file (e.g. if file is on npexp, z drive is not a backup).
        """
        if (
            self.z_drive_path
            and self.z_drive_path.exists()
            and self.z_drive_path.as_posix() != self.path.as_posix()
            and "neuropixels_data" not in self.path.parts
            and self.path != self.npexp_path
            # if file is on npexp, don't consider z drive as a backup
            and self.session.lims_path not in self.path.parents
            # if file is on lims, don't consider z drive as a backup
        ):
            return self.z_drive_path
        return None

    def get_z_drive_path(self) -> pathlib.Path:
        """Path to possible backup on 'z' drive (might not exist)"""
        # TODO add session method for getting z drive, using rigID from lims
        # then use whichever z drive exists (original vs current)
        running_on_rig = nptk.COMP_ID if "NP." in nptk.COMP_ID else None
        local_path = str(self.path)[0] not in ["/", "\\"]
        rig_from_path = nptk.Rig.rig_from_path(self.path.as_posix())

        # get the sync computer's path
        if running_on_rig and local_path:
            sync_path = nptk.Rig.Sync.path
        elif rig_from_path:
            rig_idx = nptk.Rig.rig_str_to_int(rig_from_path)
            sync_path = (
                "//"
                + nptk.ConfigHTTP.get_np_computers(rig_idx, "sync")[
                    f"NP.{rig_idx}-Sync"
                ]
            )
        else:
            sync_path = None
        # the z drive/neuropix data folder for this rig
        return (
            (
                pathlib.Path(sync_path, "neuropixels_data", self.session.folder)
                / self.session_relative_path
            )
            if sync_path
            else None
        )

    def __lt__(self, other):
        if self.session.id == other.session.id:
            return self.session_relative_path < other.session_relative_path
        return self.session.id < other.session.id

    @property
    def incoming_path(self) -> pathlib.Path:
        """Path to file in incoming folder (may not exist)"""
        return INCOMING_ROOT / self.relative_path



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
