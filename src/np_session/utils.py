from __future__ import annotations

import datetime
import doctest
import enum
import functools
import json
import os
import pathlib
import re
import subprocess
import sys
from typing import Union

from typing_extensions import Literal
from backports.cached_property import cached_property

import np_logging
import requests

import np_session.components.paths as paths
from np_session.databases.data_getters import lims_data_getter
from np_session.databases.lims2 import LIMS2SessionInfo

logger = np_logging.getLogger(__name__)

PathLike = Union[str, bytes, os.PathLike, pathlib.Path]
# https://peps.python.org/pep-0519/#provide-specific-type-hinting-support
# PathLike inputs are converted to pathlib.Path objects for os-agnostic filesystem operations
# os.fsdecode(path: PathLike) is used where only a string is required

RE_SESSION_ID = re.compile("[0-9]{8,}")
RE_FOLDER = re.compile("[0-9]{8,}_[0-9]{6}_[0-9]{8}")
RE_PROBES = re.compile("(?<=_probe)_?(([A-F]+)|([0-5]))")

REPLACED_COMP_ID: dict[str, tuple[datetime.date, str]] = {
    'NP.0-Acq': (datetime.date(2022, 10, 18), "W10DT05515"),
    'NP.1-Acq': (datetime.date(2022, 10, 27), "W10DT05501"),
    'NP.2-Acq': (datetime.date(2022, 7, 14), "W10DT05517"),
    'NP.0-Stim': (datetime.date(2023, 2, 7), "W10DT713938"),
    'NP.1-Stim': (datetime.date(2023, 1, 19), "W10DT713942"),
}

def old_hostname(comp_id: str, date: datetime.date) -> str | None:
    """Return the hostname for a computer that was replaced, if `date` predates the switchover.
    
    For a date before the hostname changed:
    >>> old_hostname('NP.1-Acq', datetime.date(2022, 6, 18))
    'W10DT05501'

    For a date after the hostname changed, nothing is returned: 
    >>> old_hostname('NP.1-Acq', datetime.date(2023, 6, 18))

    """
    if not (replaced := REPLACED_COMP_ID.get(comp_id)):
        return
    if date < replaced[0]:
        return replaced[1]

class Host(enum.Enum):
    LIMS = "lims2"
    MTRAIN = "mtrain"
    MINDSCOPE_SERVER = ZK = "eng-mindscope"
    MPE_SERVER = "aibspi"


def is_connected(host: str | Host) -> bool:
    "Use OS's `ping` cmd to check if `host` is connected."
    command = ["ping", "-n" if "win" in sys.platform else "-c", "1", host]
    try:
        return subprocess.call(command, stdout=subprocess.PIPE, timeout=0.1) == 0
    except subprocess.TimeoutExpired:
        return False


def is_lims_path(path: PathLike) -> bool:
    """
    >>> is_lims_path('//allen/programs/mindscope/production/dynamicrouting/prod0/specimen_1200280254/ecephys_session_1234028213')
    True
    """
    parts = pathlib.Path(path).parts
    return "production" in parts and "incoming" not in parts


def is_valid_session_id(session_id: int | str) -> bool:
    """
    >>> is_valid_session_id('1234028213')
    True
    >>> is_valid_session_id('abcdefg')
    False
    """
    try:
        _ = int(session_id)
    except ValueError:
        return False
    return bool(LIMS2SessionInfo(session_id))


def lims_session_id(path: PathLike) -> str | None:
    """
    Get valid lims ecephys/behavior session id from str or path.

    >>> lims_session_id('//allen/programs/mindscope/production/dynamicrouting/prod0/specimen_1200280254/ecephys_session_1234028213')
    '1234028213'
    """

    path = os.fsdecode(path)
    if is_lims_path(path):
        from_lims_path = re.search("(?<=_session_)\\d+", os.fsdecode(path))
        if from_lims_path:
            return from_lims_path.group(0)
    for i in RE_SESSION_ID.findall(path):
        if is_valid_session_id(i):
            return i


def folder(path: PathLike) -> str | None:
    """
    Extract [8+digit lims session ID]_[6-digit labtracks mouse ID]_[6-digit datestr] from a str or path.

    >>> folder('//allen/programs/mindscope/workgroups/np-exp/1234028213_640887_20221219/image.png')
    '1234028213_640887_20221219'
    """

    session_folders = RE_FOLDER.findall(os.fsdecode(path))

    if not session_folders:
        return folder_from_lims_id(path)
    if not all(s == session_folders[0] for s in session_folders):
        logger.warning(
            f"Mismatch between session folder strings - file may be in the wrong folder: {path!r}"
        )
    return session_folders[0]


def folder_from_lims_id(path: PathLike) -> str | None:
    """
    Get the session folder string ([lims-id]_[mouse-id]_[date]) from a string or path containing a possible lims id.

    >>> folder_from_lims_id('//allen/programs/mindscope/production/dynamicrouting/prod0/specimen_1200280254/ecephys_session_1234028213')
    '1234028213_640887_20221219'

    >>> folder_from_lims_id('1234028213')
    '1234028213_640887_20221219'
    """
    session_id = lims_session_id(path)
    if session_id is None:
        return None
    lims_data = lims_data_getter(session_id)
    return ("_").join(
        [
            lims_data.lims_id,
            lims_data.data_dict["external_specimen_name"],
            lims_data.data_dict["datestring"],
        ]
    )


def folder_from_lims_id(path: PathLike) -> str | None:
    """
    Get the session folder string ([lims-id]_[mouse-id]_[date]) from a string or path containing a possible lims id.

    >>> folder_from_lims_id('//allen/programs/mindscope/production/dynamicrouting/prod0/specimen_1200280254/ecephys_session_1234028213')
    '1234028213_640887_20221219'

    >>> folder_from_lims_id('1234028213')
    '1234028213_640887_20221219'
    """
    session_id = lims_session_id(path)
    if session_id is None:
        return None
    return LIMS2SessionInfo(session_id).folder


@functools.lru_cache(maxsize=None)
def lims_json_content(lims_id: int | str) -> dict | None:
    if not is_valid_session_id(lims_id):
        raise ValueError(f"{lims_id} is not a valid lims session id")
    if not is_connected("lims2"):
        raise ConnectionError("Could not connect to lims")
    for session_type in ["ecephys_sessions", "behavior_sessions"]:
        response = requests.get(f"http://lims2/{session_type}/{lims_id}.json?")
        if response.status_code == 200:
            return response.json()
    logger.warning(f"Could not find json content for lims session id {lims_id}")
    return None


def is_new_ephys_folder(path: PathLike) -> bool:
    "Contains subfolders with raw data from OpenEphys v0.6.0+ (format switched 2022 on NP.0,1,2)"
    path = pathlib.Path(path)
    if path.match("_probe*"):
        return path.match("Record Node*") or path.rglob("Record Node*")
    return bool(list(path.glob("*_probe*/Record Node*")))


def files_manifest(
        project_name: str,
        session_str: str = '',
        session_type: Literal['D1', 'D2', 'habituation'] = 'D1',
    ) -> dict[str, dict[str,str]]:
    """Return a list of files that could be entered directly into a platform json `files` key.
    - project_name: corresponds to a manifet template 
    - session_str: [lims_id]_[mouse_id]_[session_id], will replace a placeholder in a manifest template
    """
    if session_type not in ['D1', 'habituation', 'D2']:
        raise ValueError(f'{session_type} is not a valid session type')
    
    template = paths.TEMPLATES_ROOT / session_type / f"{project_name}.json"
    
    x = json.loads(template.read_bytes())
    # convert dict to str
    # replace % with session string
    # switch ' and " so we can convert str back to dict with json.loads()
    return json.loads(str(x).replace('%', str(session_str)).replace('\'','"'))


if __name__ == "__main__":

    if is_connected("lims2"):
        doctest.testmod()
        # optionflags=(doctest.ELLIPSIS, doctest.NORMALIZE_WHITESPACE, doctest.IGNORE_EXCEPTION_DETAIL)
    else:
        print("LIMS not connected - skipping doctests")
