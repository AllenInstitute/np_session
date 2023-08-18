from __future__ import annotations

import contextlib
import copy
import datetime
import doctest
import pathlib
from typing import Iterable, Optional

import np_config
import np_logging
from backports.cached_property import cached_property
from typing_extensions import Literal, Self

from np_session.components.info import Project, User, Mouse
from np_session.components.lims_manifests import Manifest
from np_session.components.paths import *
from np_session.components.platform_json import *
from np_session.databases import data_getters as dg
from np_session.databases import lims2 as lims
from np_session.databases import mtrain
from np_session.utils import *
from np_session.session import Session

logger = np_logging.getLogger(__name__)


class PipelineSession(Session):
    """Session information from any string or PathLike containing a session ID.

    Note: lims/mtrain properties may be empty or None if mouse/session isn't in db.
    Note: `is_ecephys` checks ecephys vs behavior: habs are ecephys sessions, as in lims.

    Quick access to useful properties:
    >>> session = PipelineSession('c:/1116941914_surface-image1-left.png')
    >>> session.lims.id
    1116941914
    >>> session.folder
    '1116941914_576323_20210721'
    >>> session.project.lims.id
    714916854
    >>> session.is_hab
    False
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

    @staticmethod
    def get_folder(value: int | str | PathLike) -> str | None:
        return get_lims_session_folder(value)

    @classmethod
    def new(
        cls,
        mouse: str | int | Mouse,
        user: str | User,
        session_type: Literal['ephys', 'hab', 'behavior'] = 'ephys',
    ) -> Self:
        """Create a new session in LIMS and return a Session instance."""
        if not isinstance(mouse, Mouse):
            mouse = Mouse(mouse)
        if not isinstance(user, User):
            user = User(user)
        if (
            'ephys' in session_type
        ):   # maintain backwards compatibility with 'ecephys'
            lims_session = lims.generate_ephys_session(
                mouse=mouse.lims, user=user.lims
            )
        elif session_type == 'hab':
            lims_session = lims.generate_hab_session(
                mouse=mouse.lims, user=user.lims
            )
        elif session_type == 'behavior':
            raise ValueError('Generating behavior sessions is not yet supported')
        session = cls(lims_session)
        # assign instances with data already fetched from lims:
        session._mouse = mouse
        session._user = user
        return session


    @property
    def folder(self) -> str:
        """Folder name, e.g. `[lims session ID]_[labtracks ID]_[8-digit date]`."""
        return self._folder

    @folder.setter
    def folder(self, value: str | PathLike) -> None:
        folder = self.get_folder(value)
        if folder is None:
            raise ValueError(
                f'Session folder must be in the format `[lims session ID]_[6-digit mouse ID]_[8-digit date str]`: {value}'
            )
        self._folder = folder

    @property
    def id(self) -> int:
        """LIMS session ID (ephys for ecephys/hab, behavior for behavior)"""
        return int(self.folder.split('_')[0])

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
        if not hasattr(self, '_lims'):
            try:
                self._lims = lims.LIMS2SessionInfo(self.id)
            except ValueError:
                self._lims = {}
        return self._lims

    @lims.setter
    def lims(self, value: lims.LIMS2SessionInfo):
        if not isinstance(value, lims.LIMS2SessionInfo):
            raise TypeError(
                f'Expected `lims` to be an instance of `lims.LIMS2SessionInfo`, not {type(value)}'
            )
        self._lims = value

    @property
    def user(self) -> User | None:
        if not hasattr(self, '_user'):
            lims_user_id = self.lims.get('operator', {}).get('login', '')
            if lims_user_id:
                self._user = User(lims_user_id)
            else:
                self._user = None
        return self._user

    @property
    def rig(self) -> np_config.Rig | None:
        """Rig object with computer info and paths, can also be used as a string."""
        if not hasattr(self, '_rig'):
            self._rig = None
            while not self.rig:
                # try from current rig first
                with contextlib.suppress(ValueError):
                    self.rig = np_config.Rig()
                    continue

                # try from platform json
                with contextlib.suppress(Exception):
                    self.rig = np_config.Rig(self.platform_json.rig_id)
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
            raise TypeError(
                f'Expected `rig` to be an instance of `np_config.Rig`, not {type(value)}'
            )
        self._rig = value
        self.update_hostnames_for_replaced_computers()

    def update_hostnames_for_replaced_computers(self) -> None:
        if not self._rig:
            return
        for comp in ('sync', 'stim', 'mon', 'acq'):
            replaced = old_hostname(
                f'{self._rig.id}-{comp.capitalize()}', self.date
            )
            if replaced:
                setattr(self._rig, f'_{comp}', replaced)

    @property
    def is_ecephys(self) -> bool | None:
        """False if behavior session in lims, None if unsure.

        Note that habs are classed as ecephys sessions: use `is_hab`.
        """
        if not self.lims or not self.lims.get('ecephys_session'):
            return None
        return 'ecephys_session' in self.lims['ecephys_session']

    @property
    def is_hab(self) -> bool | None:
        """False if hab session, None if unsure."""
        if not self.lims:
            return None
        return self.lims.get('name', '').startswith('HAB')

    @property
    def npexp_path(self) -> pathlib.Path:
        """np-exp root / folder (may not exist)"""
        return (
            NPEXP_ROOT / 'habituation' / self.folder
            if self.is_hab
            else NPEXP_ROOT / self.folder
        )

    @property
    def lims_path(self) -> pathlib.Path | None:
        """Corresponding directory in lims, if one can be found"""
        if not hasattr(self, '_lims_path'):
            path: str = self.lims.get('storage_directory', '')
            if not path:
                logger.debug(
                    'lims checked successfully, but no folder uploaded for %s',
                    self.id,
                )
                self._lims_path = None
            else:
                self._lims_path = pathlib.Path('/' + path)
        return self._lims_path

    @property
    def z_path(self) -> pathlib.Path:
        """Path in Sync neuropixels_data (aka Z:) (may not exist))"""
        return (
            np_config.local_to_unc(
                self.rig.sync, NEUROPIXELS_DATA_RELATIVE_PATH
            )
            / self.folder
        )

    @cached_property
    def qc_paths(self) -> list[pathlib.Path]:
        """Any QC folders that exist"""
        return [
            path / self.folder
            for path in QC_PATHS
            if (path / self.folder).exists()
        ] + ([self.qc_path] if self.qc_path.exists() else [])

    @property
    def project(self) -> Project | None:
        if not hasattr(self, '_project'):
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
            logger.debug('Connection to lims failed', exc_info=True)
            return None
        except:
            raise

    @property
    def data_dict(self) -> dict:
        if not hasattr(self, '_data_dict'):
            data_getter = self.lims_data_getter
            if not data_getter:
                self._data_dict = {}
            else:
                self._data_dict_orig = data_getter.data_dict  # str paths
                self._data_dict = (
                    data_getter.data_dict_pathlib
                )  # pathlib paths
        return self._data_dict

    @property
    def mtrain(self) -> mtrain.MTrain | dict:
        """Info from MTrain on the last behavior session for the mouse on the experiment day"""
        if not hasattr(self, '_mtrain'):
            if not is_connected('mtrain'):
                return {}
            try:
                _ = self.mouse.mtrain
            except mtrain.MouseNotInMTrainError:
                self._mtrain = {}
            except:
                raise
            else:
                self._mtrain = self.mouse.mtrain.last_behavior_session_on(
                    self.date
                )
        return self._mtrain

    @property
    def foraging_id(self) -> str | None:
        """From lims, mtrain, or platform json, in that order."""
        if not hasattr(self, '_foraging_id'):
            self._foraging_id = (
                self.foraging_id_lims
                or self.foraging_id_mtrain
                or (
                    self.platform_json.foraging_id
                    if self.platform_json.file_sync
                    else None
                )
            )
        return self._foraging_id

    @foraging_id.setter
    def foraging_id(self, value: str) -> None:
        self.platform_json.foraging_id = value   # validates uuid
        self._foraging_id = value

    @cached_property
    def foraging_id_mtrain(self) -> str | None:
        """Foraging ID from MTrain (if an MTrain session is found)."""
        return self.mtrain.get('id', None)

    @cached_property
    def foraging_id_lims(self) -> str | None:
        """Foraging ID from lims based on start/stop time of experiment and mouse ID
        (from platform json), obtained from the behavior session that ran at the time.

        Not all mice have foraging IDs (e.g. variability project)"""
        try:
            from_lims = dg.get_foraging_id_from_behavior_session(
                self.mouse.id,
                self.start,
                self.end,
            )
        except (dg.MultipleBehaviorSessionsError, dg.NoBehaviorSessionError):
            return None
        else:
            return from_lims

    def find_platform_json(self) -> pathlib.Path | None:
        """Find the platform.json file for this session, if it exists."""
        path = self.data_dict.get('EcephysPlatformFile') or next(
            self.npexp_path.glob('*platformD1*.json'), None
        )
        if path and 'platformD1' in path.name:
            return np_config.normalize_path(path)

    def find_settings_xml(self) -> pathlib.Path | None:
        """Find one of the settings.xml files for this session.

        Files associated with probes ABC and DEF are identical, so return either.
        """
        path = self.data_dict.get(
            'EcephysProbeRawDataABC_settings'
        ) or self.data_dict.get('EcephysProbeRawDataDEF_settings')
        if (not path or path.suffix != '.xml') and self.npexp_path.exists():
            path = next(self.npexp_path.glob('*_probe???/settings*.xml'), None)
        return np_config.normalize_path(path) if path else None

    @property
    def D0(self) -> Manifest:
        """D0 upload manifest for platform json, plus extra methods for finding missing files."""
        with contextlib.suppress(AttributeError):
            return self._D0
        self._D0 = Manifest(self, 'D0')
        return self.D0

    @property
    def D1(self) -> Manifest:
        """D1 upload manifest for platform json, plus extra methods for finding missing files."""
        with contextlib.suppress(AttributeError):
            return self._D1
        self._D1 = Manifest(self, 'D1')
        return self.D1

    @property
    def D2(self) -> Manifest:
        """D2 upload manifest for platform json, plus extra methods for finding missing files."""
        with contextlib.suppress(AttributeError):
            return self._D2
        self._D2 = Manifest(self, 'D2')
        return self.D2

    def get_missing_files(self) -> tuple[str, ...]:
        """Globs for D1 & D2 files that are missing from npexp"""
        missing_globs = [
            self.D1.globs[self.D1.names.index(_)] for _ in self.D1.missing
        ]
        missing_globs.extend(
            self.D2.globs[self.D2.names.index(_)] for _ in self.D2.missing
        )
        missing_globs.extend(
            self.D2.globs_sorted_data[self.D2.names_sorted_data.index(_)]
            for _ in self.D2.missing_sorted_data
            if not any(f'probe_{_[-1]}' in __ for __ in self.D2.missing)
        )   # don't add each individual missing sorted file if we already added their parent probeX_sorted folder
        return tuple(dict.fromkeys(missing_globs))

    @cached_property
    def metrics_csv(self) -> tuple[pathlib.Path, ...]:
        probe_letters = self.data_dict.get('data_probes')
        probe_letters = self.probes_inserted
        probe_paths = [
            self.data_dict.get(f'probe{letter}') for letter in probe_letters
        ]
        if any(probe_paths):
            csv_paths = [
                _ / 'metrics.csv'
                for _ in probe_paths
                if _ and (_ / 'metrics.csv').exists()
            ]
            if csv_paths:
                return tuple(csv_paths)
        return tuple(self.npexp_path.glob('*/*/*/metrics.csv'))

    @property
    def platform_json(self) -> PlatformJson:
        """Platform D1 json on npexp."""
        with contextlib.suppress(AttributeError):
            self._platform_json.load_from_existing()
            return self._platform_json
        self._platform_json = PlatformJson(self.npexp_path)
        update_from_session(self._platform_json, self)
        return self._platform_json

    def fix_platform_json(
        self, path_or_obj: Optional[pathlib.Path | PlatformJson] = None
    ):
        if not path_or_obj:
            path_or_obj = self.platform_json
        if isinstance(path_or_obj, pathlib.Path):
            path_or_obj = PlatformJson(path_or_obj)
        # TODO get files dict, fetch files

    @cached_property
    def start(self) -> datetime.datetime:
        """Start time estimated from platform.json, for finding files created during
        experiment. Not relevant for D2 files.

        In the event that the platform.json file does not contain a time, we use the start
        of the day of the session.
        """
        if self.platform_json.file_sync:
            fields_to_try = (
                'ExperimentStartTime',
                'ProbeInsertionStartTime',
                'CartridgeLowerTime',
                'HeadFrameEntryTime',
                'workflow_start_time',
            )
            for _ in fields_to_try:
                time = getattr(self.platform_json, _)
                if isinstance(time, datetime.datetime):
                    return time
            logger.debug(
                'Could not find experiment start time in %s: using start of day instead',
                self.platform_json,
            )
        return super().start

    @cached_property
    def end(self) -> datetime.datetime:
        """End time estimated from platform.json, for finding files created during
        experiment. Not relevant for D2 files.

        In the event that the platform.json file does not contain a time, we use the end
        of the day of the session.
        """
        if self.platform_json.file_sync:
            fields_to_try = (
                'workflow_complete_time',
                'ExperimentCompleteTime',
                'HeadFrameExitTime',
            )
            for _ in fields_to_try:
                time = getattr(self.platform_json, _)
                if isinstance(time, datetime.datetime):
                    return time
            logger.debug(
                'Could not find experiment end time in %s: using end of day instead',
                self.platform_json,
            )
        return super().end

    @property
    def experiment_start(self):
        """For backwards compatibility with old code.
        Replace with `self.start`.
        """
        return self.start()

    @property
    def experiment_end(self):
        """For backwards compatibility with old code.
        Replace with `self.end`.
        """
        return self.end()

    @property
    def probes_inserted(
        self,
    ) -> Optional[tuple[Literal['A', 'B', 'C', 'D', 'E', 'F'], ...]]:
        probes = 'ABCDEF'
        notes: dict = self.platform_json.InsertionNotes
        # assume that no notes means probe was inserted
        return tuple(
            _
            for _ in probes
            if (f'Probe{_}' not in notes)
            or (notes[f'Probe{_}'].get('FailedToInsert') == 0)
        )

    @probes_inserted.setter
    def probes_inserted(
        self, inserted: str | Iterable[Literal['A', 'B', 'C', 'D', 'E', 'F']]
    ):
        probes = 'ABCDEF'
        inserted = ''.join(_.upper() for _ in inserted)
        if not all(_ in probes for _ in inserted):
            raise ValueError(
                f'Probes must be a sequence of letters A-F, got {inserted}'
            )
        notes = copy.deepcopy(self.platform_json.InsertionNotes)
        for _ in probes:
            probe = f'Probe{_}'
            if probe in notes and _ in inserted:
                notes[probe]['FailedToInsert'] = 0
            if probe not in notes and _ not in inserted:
                notes[probe] = {'FailedToInsert': 1}
        self.platform_json.InsertionNotes = notes
        logger.debug(
            'Updated %s InsertionNotes: %s',
            self.platform_json,
            self.platform_json.InsertionNotes,
        )

    @cached_property
    def probe_letter_to_serial_number_from_probe_info(
        self,
    ) -> dict[str, int | None]:
        """Probe letter to serial number, if they can be found from `probe_info.json`.

        Not a tuple because we might not find a serial number for all probes.
        """
        probe_letters: list[str] = self.data_dict.get('data_probes')
        probe_info = [
            self.data_dict.get(f'probe{letter}_info')
            for letter in probe_letters
        ]
        if not any(probe_info):
            return {}
        mapping = dict().fromkeys(probe_letters, None)
        for letter, info in zip(probe_letters, probe_info):
            result = (
                json.loads(pathlib.Path(info).read_bytes())
                .get('probe', {})
                .get('serial number')
            )
            mapping[letter] = int(result) if result else None


if __name__ == '__main__':
    if is_connected('lims2'):
        doctest.testmod(verbose=True)
        optionflags = (
            doctest.ELLIPSIS,
            doctest.NORMALIZE_WHITESPACE,
            doctest.IGNORE_EXCEPTION_DETAIL,
        )
    else:
        print('LIMS not connected - skipping doctests')
