from __future__ import annotations
import datetime
import doctest
import enum
import functools
import logging
import os
import pathlib
import platform
import re
import subprocess
import sys
from typing import Dict, Optional, Union

import requests

if __name__ == "__main__":
    from data_getters import lims_data_getter
    from lims2 import SessionInfo
else:
    from .data_getters import lims_data_getter
    from .lims2 import SessionInfo


PathLike = Union[str, bytes, os.PathLike, pathlib.Path]
# https://peps.python.org/pep-0519/#provide-specific-type-hinting-support
# PathLike inputs are converted to pathlib.Path objects for os-agnosticfilesystem operations
# os.fsdecode(path: PathLike) is used where only a string is required

RE_SESSION_ID = re.compile("[0-9]{8,}")
RE_FOLDER = re.compile("[0-9]{8,}_[0-9]{6}_[0-9]{8}")
RE_PROBES = re.compile("(?<=_probe)_?(([A-F]+)|([0-5]))")


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
    """
    return bool(lims_data_getter(str(session_id)).data_dict)


def lims_session_id(path: PathLike) -> Optional[str]:
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



def folder(path: PathLike) -> Optional[str]:
    """
    Extract [8+digit lims session ID]_[6-digit labtracks mouse ID]_[6-digit datestr] from a str or path.
    
    >>> folder('//allen/programs/mindscope/workgroups/np-exp/1234028213_640887_20221219/image.png')
    '1234028213_640887_20221219'
    """

    session_folders = RE_FOLDER.findall(os.fsdecode(path))

    if not session_folders:
        return folder_from_lims_id(path)
    if not all(s == session_folders[0] for s in session_folders):
        logging.warning(
            f"Mismatch between session folder strings - file may be in the wrong folder: {path}"
        )
    return session_folders[0]


def folder_from_lims_id(path: PathLike) -> Optional[str]:
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
        [lims_data.lims_id, lims_data.data_dict["external_specimen_name"], lims_data.data_dict["datestring"]]
    )
    
def folder_from_lims_id(path: PathLike) -> Optional[str]:
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
    return SessionInfo(session_id).folder

@functools.lru_cache(maxsize=None)
def lims_json_content(lims_id: int | str) -> Optional[Dict]:
    if not is_valid_session_id(lims_id):
        raise ValueError(f"{lims_id} is not a valid lims session id")
    if not is_connected("lims2"):
        raise ConnectionError("Could not connect to lims")
    for session_type in ["ecephys_sessions", "behavior_sessions"]:
        response = requests.get(f"http://lims2/{session_type}/{lims_id}.json?")
        if response.status_code == 200:
            return response.json()
    logging.warning(f"Could not find json content for lims session id {lims_id}")
    return None

    
if __name__ == "__main__":
    try:
        # optionflags=(doctest.ELLIPSIS, doctest.NORMALIZE_WHITESPACE, doctest.IGNORE_EXCEPTION_DETAIL)
        doctest.testmod()
    except ConnectionError:
        print("not on-site - skipping doctests")