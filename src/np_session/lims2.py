"""
Critical components of an NP experiment workflow that come from lims2.

- user/operator info
- mouse info 
    - isi experiment
    - project name

Combined, these are sufficient to create a new ecephys session entry in lims2.

Tools are also provided to construct commonly used abbreviations or folder names, and to
reverse these 'NP' formats to reconstruct the Info objects used during an
experiment workflow.

"""
from __future__ import annotations

import abc
import collections
import datetime
import functools
import json
import logging
import pathlib
import re
from typing import Any, Callable, Type, Union

import requests


def requester(url: str, *args) -> dict:
    request = url.format(*args)  # .replace(";", "%3B")
    logging.debug(f"Requesting {request}")
    response = requests.get(request)
    if response.status_code != 200:
        raise ValueError(f"Bad response from {request}: {response.status_code}")
    return response.json()


request = lambda url: functools.partial(requester, url)
donor_info = request("http://lims2/donors/info/details.json?external_donor_name={}")
user_info = request("http://lims2/users.json?login={}")
ecephys_info = request("http://lims2/ecephys_sessions.json?id={}")
behavior_info = request("http://lims2/behavior_sessions.json?id={}")
isi_info = request("http://lims2/specimens/isi_experiment_details/{}.json")


class LIMS2InfoBaseClass(collections.UserDict, abc.ABC):
    "Store details for an object in a dict-like. The commonly-used format of its name, e.g. '366122' for a mouse ID, can be obtained by converting to str()."

    np_id: int | str
    "Commonly-used format of the object's value among the neuropixels team e.g. for a mouse -> the labtracks ID (366122)."

    _type: Type = NotImplemented
    _get_info: Callable[[str | int], dict] = NotImplemented

    def __init__(self, np_id: str | int):
        self.np_id = self.__class__._type(np_id)
        super().__init__()

    def __getitem__(self, key):
        self.fetch()
        return super().__getitem__(key)

    def __str__(self):
        return str(self.np_id)

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.np_id}')"

    def __bool__(self):
        return True if self.np_id else False

    def info_from_lims(self, np_id) -> dict[str, Any]:
        "Return the object's info from lims database or raises a ValueError if not found."
        try:
            return self._get_info(np_id)[0]
        except IndexError:
            raise ValueError(
                f"Could not find {self.__class__.__name__} {np_id} in lims"
            ) from None

    def fetch(self):
        "Fetch the object's info from lims once."
        if not self.data:
            info = self.info_from_lims(self.np_id)
            self.data = dict(**info)

    def keys(self):
        "Fetch before returning keys."
        self.fetch()
        return self.data.keys()

    def items(self):
        "Fetch before returning keys."
        self.fetch()
        return self.data.items()

    def values(self):
        "Fetch before returning keys."
        self.fetch()
        return self.data.values()

    @abc.abstractproperty
    def lims_id(self):
        "LIMS2 ID for the object, usually different to the np_id."
        return NotImplemented

    # end of baseclass properties & methods ------------------------------ #


class MouseInfo(LIMS2InfoBaseClass):
    """
    Mouse info from lims stored in a dict, with a string method for the commonly-used format of its name.

    >>> mouse = MouseInfo(366122)

    >>> str(mouse)
    '366122'

    >>> mouse['id']
    657428270

    >>> mouse['project_name']
    'NeuropixelPlatformDevelopment'
    """

    _type = int
    _get_info = donor_info

    @property
    def lims_id(self) -> int:
        return self["specimens"][0]["id"]

    @property
    def isi_info(self) -> dict | None:
        "Info from lims about the mouse's ISI experiments."
        if not hasattr(self, "_isi_info"):
            response = isi_info(str(self.np_id))
            if response:
                self._isi_info = response[0]
            else:
                self._isi_info = None
        return self._isi_info

    @property
    def isi_id(self) -> int | None:
        "ID of the mouse's most recent ISI experiment not marked `failed`."
        exps: list = self.isi_info["isi_experiments"]
        exps.sort(key=lambda x: x["id"], reverse=True)
        for exp in exps:
            if exp["workflow_state"] != "failed":
                return exp["id"]
        return None

    @property
    def project_id(self) -> int:
        "ID of the the project the mouse belongs to."
        return self["specimens"][0]["project_id"]

    @property
    def project_name(self) -> str:
        "PascalCase name of the project the mouse belongs to."
        return self["specimens"][0]["project"]["code"]

    @property
    def path(self) -> pathlib.Path:
        "Allen network dir where the mouse's sessions are stored."
        return pathlib.Path(self["specimens"][0]["storage_directory"])


class UserInfo(LIMS2InfoBaseClass):
    "Store details for a user/operator."

    _type = str
    _get_info = user_info

    @property
    def lims_id(self) -> int:
        return self["id"]


class SessionInfo(LIMS2InfoBaseClass):
    "Store details for an ecephys or behavior session."

    _type = int
    _get_info = ecephys_info  # default - behavior_info may be used instead

    def __str__(self):
        return f"{self.np_id}"

    @property
    def lims_id(self) -> int:
        return self.np_id

    @property
    def session(self) -> str:
        self.cast()
        return self._session

    @property
    def folder(self):
        self.cast()
        return self.get_folder()

    def info_from_lims(self, np_id) -> dict:
        try:
            data = super().info_from_lims(np_id)  # with ecephys_info
        except ValueError:
            response = behavior_info(np_id)  # try behavior_info instead
            if not response:
                raise
            self._session = "behavior"
            data = response[0]
        else:
            self._session = "ecephys"
        self.cast()
        return data

    def cast(self):
        if not hasattr(self, "_session"):
            _ = self["id"]  # trigger fetching data from lims
        self.__class__ = (
            EcephysSessionInfo if self._session == "ecephys" else BehaviorSessionInfo
        )


class EcephysSessionInfo(SessionInfo):
    "Don't instantiate directly, use SessionInfo and class will be converted after info fetched from lims."

    def get_folder(self) -> str:
        return f"{self.np_id}_{self['specimen']['external_specimen_name']}_{self['name'][:8]}"

    def __repr__(self):
        return f"{__class__.__bases__[0].__name__}({self.np_id!r})"


class BehaviorSessionInfo(SessionInfo):
    "Don't instantiate directly, use SessionInfo and class will be converted after info fetched from lims."

    def get_folder(self) -> str:
        return f"{self.np_id}_{self['donor']['name'].split('-')[-1]}_{self['ecephys_session']['name'][:8]}"

    def __repr__(self):
        return f"{__class__.__bases__[0].__name__}({self.np_id!r})"


# end of classes ----------------------------------------------------------------------- #


def generate_ecephys_session(
    mouse: str | int | MouseInfo,
    user: str | UserInfo,
) -> SessionInfo:
    "Create a new session and return an object instance with its info."

    if not isinstance(mouse, MouseInfo):
        mouse = MouseInfo(mouse)
    if not isinstance(user, UserInfo):
        user = UserInfo(user)

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    request_json = {
        "specimen_id": mouse.lims_id,
        "project_id": mouse.project_id,
        "isi_experiment_id": mouse.isi_id,
        "name": f"{timestamp}_{user.lims_id}",
        "operator_id": user.lims_id,
    }
    url = "http://lims2/observatory/ecephys_session/create"
    response = requests.post(url, json=request_json)
    decoded_dict = json.loads(response.content.decode("utf-8"))
    new_session_id = decoded_dict["id"]
    if not new_session_id:
        raise ValueError(f"Failed to create session: {decoded_dict}")
    return SessionInfo(new_session_id)


def find_session_folder_string(path: Union[str, pathlib.Path]) -> str | None:
    """Extract [8+digit session ID]_[6-digit mouse ID]_[6-digit date
    str] from a file or folder path"""
    session_reg_exp = r"[0-9]{8,}_[0-9]{6}_[0-9]{8}"
    session_folders = re.findall(session_reg_exp, str(path))
    if session_folders:
        if not all(s == session_folders[0] for s in session_folders):
            raise ValueError(
                f"Mismatch between session folder strings - file may be in the wrong folder: {path}"
            )
        return session_folders[0]
    return None


def info_classes_from_session_folder(
    session_folder: str,
) -> tuple[SessionInfo, MouseInfo, UserInfo]:
    "Reconstruct Info objects from a session folder string."
    folder = find_session_folder_string(session_folder)
    if not folder:
        raise ValueError(f"{session_folder} is not a valid session folder")

    session = SessionInfo(folder.split("_")[0])
    mouse = MouseInfo(folder.split("_")[1])
    user = UserInfo(session["operator"]["login"])

    return (session, mouse, user)


if __name__ == "__main__":
    mouse = MouseInfo(366122)
    e, *_ = info_classes_from_session_folder("1190094328_611166_20220707")
    print(e.keys())
