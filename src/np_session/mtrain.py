from __future__ import annotations
import datetime
import json
import warnings
from typing import Optional, Union

import requests


class MouseNotInMTrainError(Exception):
    pass


class MTrain:

    server = "http://mtrain:5000"

    @classmethod
    def connected(cls) -> bool:
        response = requests.get(cls.server)
        return True if response.status_code == 200 else False

    def __init__(self, mouse_id: Optional[int | str] = None):
        # we'll only allow mouse_id to be set once per instance
        # but it doesn't necessarily have to be set on init
        if mouse_id:
            self.mouse_id = str(mouse_id)

    def session(self):
        session = requests.session()
        session.post(
            self.server,
            data={
                "username": "chrism",
                "password": "password",
            },
        )
        return session

    @property
    def mouse_id(self) -> str:
        return self._mouse_id

    @mouse_id.setter
    def mouse_id(self, value: int | str):
        if hasattr(self, "_mouse_id") and self._mouse_id is not None:
            raise ValueError("Mouse ID can only be set once per instance.")
        response = requests.get(
            f"{self.server}/get_script/", data=json.dumps({"LabTracks_ID": str(value)})
        )
        if response.status_code == 200:
            self._mouse_id = str(value)
        else:
            raise MouseNotInMTrainError(f"Mouse ID {value} not found in MTrain")

    @property
    def state(self) -> dict[str, int]:
        """Returns dict with values 'id', 'regimen_id', 'stage_id' - all ints"""
        return requests.get(f"{self.server}/api/v1/subjects/{self.mouse_id}").json()[
            "state"
        ]

    @state.setter
    def state(self, value: Union[dict, int, str]):
        """Allows switching to one of the states in the current regimen.

        To change regimen, use set_regimen_and_stage()

        Requires a state dict, state id (str/int) or stage name (str)
        """
        valid_dict = valid_id = None

        if isinstance(value, dict):
            if value in self.states:
                valid_dict = value
            elif (
                hasattr(value, "regimen_id")
                and (str(value["regimen_id"]) in self.all_regimens().keys())
                and not (value["regimen_id"] == self.regimen["id"])
            ):
                warnings.warn(
                    "Trying to change regimen via set state method: use 'obj.regimen=...' instead"
                )
                return

        elif isinstance(value, int) or value.isdigit():
            # check if input matches a state id
            valid_id = (
                int(value)
                if str(value) in [str(s["id"]) for s in self.states]
                else None
            )

        elif isinstance(value, str) and value.lower() in [
            str(s["name"]).lower() for s in self.stages
        ]:
            # check if input matches a stage name
            valid_id = [s["id"] for s in self.stages if s["name"] == value][0]

        if not any([valid_dict, valid_id]):
            warnings.warn(
                f"No matching state found in regimen {self.regimen['name']}. Check obj.states"
            )
            return

        if valid_id and not valid_dict:
            value = [s for s in self.states if s["id"] == value][0]

        with self.session() as s:
            s.post(
                f"{self.server}/set_state/{self.mouse_id}",
                data={
                    "state": json.dumps(value),
                },
            )
        assert self.state == value, "set state failed!"

    @property
    def states(self):
        return self.regimen["states"]

    @property
    def regimen(self) -> dict:
        """Returns dictionary containing 'id', 'name', 'stages', 'states'"""
        return requests.get(
            f"{self.server}/api/v1/regimens/{self.state['regimen_id']}"
        ).json()

    @property
    def all_behavior_sessions(self) -> dict:
        """Returns dictionary containing details of all behavior sessions for mouse_id"""
        result = requests.get(
            f"{self.server}/get_behavior_sessions/",
            data=json.dumps({"LabTracks_ID": str(self.mouse_id)}),
        )

        if result:
            return result.json()["results"]
        return None

    def last_behavior_session_on(self, query_date: datetime.date) -> dict:
        all_behavior_sessions = self.all_behavior_sessions
        matching_sessions = []
        for session in all_behavior_sessions:
            session_datetime = datetime.datetime.strptime(
                session["date"], "%a, %d %b %Y %H:%M:%S %Z"
            )
            if session_datetime.date() == query_date:
                matching_sessions.append(session)

        if matching_sessions:
            return matching_sessions[-1]
        return None

    @regimen.setter
    def regimen(self):
        warnings.warn(
            "Setting the regimen is disabled: use 'obj.set_regimen_and_stage()' to explicitly change both together"
        )

    def set_regimen_and_stage(
        self, regimen: Union[dict, int, str] = None, stage: Union[dict, int, str] = None
    ):
        """Requires a regimen dict, id (str/int) or name (str), plus stage dict, id (str/int) or name (str)"""

        def warn_and_return():
            warnings.warn(
                "Regimen not changed: invalid input. Provide an identifier for both a regimen and a stage."
            )
            return

        if not regimen and stage:
            warn_and_return()

        # we're not setting the regimen directly - we just need some info to set a valid state
        regimen_id: int = None
        stage_id: int = None

        if isinstance(regimen, dict):
            if regimen in self.get_all("regimens"):
                # valid dict provided
                regimen_id = regimen["id"]
            else:
                warn_and_return()

        elif str(regimen) in self.all_regimens().keys():
            # valid id provided
            regimen_id = int(regimen)

        elif any(str(regimen) == s.lower() for s in self.all_regimens().values()):
            # valid name provided
            regimen_id = [
                s["id"]
                for s in self.get_all("regimens")
                if s["name"].lower() == str(regimen).lower()
            ][0]

        else:
            warn_and_return()

        # get the stages available in the new regimen, without setting anything yet
        # new_regimen = self.get_all("regimens")['id'==regimen_id]
        new_regimen = [s for s in self.get_all("regimens") if s["id"] == regimen_id][0]
        new_stages = new_regimen["stages"]

        if isinstance(stage, dict):
            if stage in new_stages:
                stage_id = stage["id"]  # valid dict provided
            else:
                warn_and_return()

        elif str(stage) in [str(s["id"]) for s in new_stages]:
            stage_id = int(stage)  # valid id provided

        elif str(stage).lower() in [str(s["name"]).lower() for s in new_stages]:
            stage_id = [s["id"] for s in new_stages if s["name"] == stage][
                0
            ]  # valid name provided

        else:
            warn_and_return()

        # look for corresponding state
        state_match = [
            s
            for s in new_regimen["states"]
            if s["regimen_id"] == regimen_id and s["stage_id"] == stage_id
        ]
        if not state_match or len(state_match) != 1:
            warn_and_return()
        new_state = state_match[0]

        # set directly instead of using state set method (which intentionally blocks setting a state dict with a new regimen)
        with self.session() as s:
            s.post(
                f"{self.server}/set_state/{self.mouse_id}",
                data={
                    "state": json.dumps(new_state),
                },
            )
        assert self.state == new_state, "set regimen and stage failed!"

    @property
    def stage(self):
        for item in self.stages:
            if item["id"] == self.state["stage_id"]:
                return item

    @stage.setter
    def stage(self, value: Union[str, int]):
        """Accepts stage id (int/str) or name (str)"""

        state_match = None

        if isinstance(value, int) or value.isdigit():
            value = int(value)
            state_match = [s for s in self.states if s["stage_id"] == value]

        if isinstance(value, str):
            stage_match = [s for s in self.stages if s["name"].lower() == value.lower()]
            state_match = [
                s for s in self.states if s["stage_id"] == stage_match[0]["id"]
            ]

        if not state_match or len(state_match) != 1:
            warnings.warn(
                f'No state with stage name or id {value} found in regimen {self.regimen["name"]}. Check obj.stages or obj.states'
            )
            return

        self.state = state_match[0]

    @property
    def script(self) -> dict:
        """Re-routes to stage property"""
        return self.stage

    @script.setter
    def script(self, value: Union[str, int]) -> dict:
        """Accepts stage id (int/str) or name (str)
        Re-routes to stage property
        """
        self.stage = value

    @property
    def stages(self):
        return self.regimen["stages"]

    @classmethod
    def paginated_get(cls, route, page_size=10, offset=0):
        page_number = offset + 1  # page number is 1 based, offset is page
        result = requests.get(
            f"{route}?results_per_page={page_size}&page={page_number}"
        ).json()
        total_pages = result["total_pages"]
        if page_number == total_pages:
            new_offset = None
            has_more = False
        else:
            new_offset = offset + 1
            has_more = True

        return result["objects"], has_more, new_offset

    @classmethod
    def get_all(cls, endpoint: str):
        # page_size = 200 is over the actual page size limit for the api but we don't appear to know what that value is
        # ? 'total_pages': 18
        if endpoint not in ["regimens", "states", "stages", "subjects"]:
            raise ValueError(f"Endpoint {endpoint} not recognized")
        all = []
        max_fetch = 100
        page_size = 200
        offset = 0
        for _ in range(max_fetch):
            results, has_more, new_offset = cls.paginated_get(
                f"{cls.server}/api/v1/{endpoint}", page_size, offset
            )
            all.extend(results)
            if not has_more:
                break
            offset = new_offset
        else:
            warnings.warn(f"Failed to get full list of {endpoint}.")
        return all

    @classmethod
    def all_regimens(cls) -> dict:
        """List of dicts {str(regimen['id']):regimen['name']}"""
        d = {}
        for val in cls.get_all("regimens"):
            d.update({str(val["id"]): val["name"]})
        return d

    # the two methods below were added after the others above:
    # other functions could be re-written to use them for cleaner code

    @classmethod
    def all_states(cls) -> dict:
        """dict containing {str(state['id']):{'id':int,'regimen_id':int,'stage_id':int}}"""
        d = {}
        for val in cls.get_all("states"):
            d.update({str(val["id"]): val})
        return d

    @classmethod
    def all_stages(cls) -> dict:
        """Takes a while. dict containing {str(stage['id']):stage['name']}"""
        d = {}
        for val in cls.get_all("stages"):
            d.update({str(val["id"]): val["name"]})
        return d


if __name__ == "__main__":
    # print(MTrain.connected())
    x = MTrain("632296")
    x.last_behavior_session_on(query_date=datetime.date(2022, 9, 27))
    x.state = x.regimen["states"][0]
    x.state = 1734
    x.stage = 1751
    illusion_regimens = [r for r in x.get_all("regimens") if "Illusion" in r["name"]]
    # x.regimen = x.get_all("regimens")[4]
    x.all_stages()
    x.all_states()
    # x.set_regimen_and_stage(illusion_regimens[4], illusion_regimens[4]['stages'][4])
