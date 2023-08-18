from __future__ import annotations

import contextlib
import datetime
import doctest
import itertools
import os
import pathlib
from typing import Any, Iterable, Optional, Type, TypeVar, Union

from backports.cached_property import cached_property
import np_config
import np_logging
from typing_extensions import Literal, Self

from np_session.components.info import Mouse, Project, User
from np_session.components.mixins import WithState
from np_session.components.paths import *
from np_session.components.platform_json import *
from np_session.utils import *
from np_session.exceptions import SessionError

logger = np_logging.getLogger(__name__)

PathLike = Union[str, bytes, os.PathLike, pathlib.Path]
# https://peps.python.org/pep-0519/#provide-specific-type-hinting-support
# PathLike inputs are converted to pathlib.Path objects for os-agnostic filesystem operations.
# os.fsdecode(path: PathLike) is used where only a string is required.


SessionT = TypeVar('SessionT', bound='Session')


class Session(WithState):
    """Session information from any string or PathLike containing a session ID.

    Note: lims/mtrain properties may be empty or None if mouse/session isn't in db.
    Note: `is_ecephys` checks ecephys vs behavior: habs are ecephys sessions, as in lims.

    Quick access to useful properties:
    >>> session = Session('c:/1116941914_surface-image1-left.png')
    >>> session == Session(session) # init with Session instance acceptable
    True
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

    def __init__(self, path_or_session: PathLike | int | LIMS2SessionInfo):
        try:
            # assign directly and folder setter will validate for us
            self.folder = str(path_or_session)
        except ValueError as exc:
            raise SessionError(
                f'{path_or_session} does not contain a valid {self.__class__.__name__} session id or session folder string'
            ) from exc

        if isinstance(path_or_session, LIMS2SessionInfo):
            self.lims = path_or_session

    def __new__(cls, *args, **kwargs) -> Self:
        """Initialize a Session object from any string or PathLike containing a
        lims session ID.

        Class will be cast as a Session subclass type as appropriate.
        """
        if cls is __class__:
            subclass = __class__.subclass_from_factory(*args, **kwargs)
            new = object.__new__(subclass)
            if __name__ == '__main__':
                new.__init__(*args, **kwargs)
            return new
        return object.__new__(cls)

    @staticmethod
    def subclass_from_factory(*args, **kwargs) -> Type[Session]:
        if isinstance(args[0], __class__):
            return args[0].__class__
        import np_session.subclasses as subclasses

        if __name__ == '__main__':
            from np_session.session import Session as cls
        else:
            cls = __class__
        for subclass in (
            *__class__.__subclasses__(),
            *cls.__subclasses__(),
            *(
                _
                for _ in subclasses.__dict__.values()
                if isinstance(_, type) and issubclass(_, (__class__, cls))
            ),
        ):
            for value in (*args, kwargs.values()):
                if subclass.get_folder(str(value)) is not None:
                    logger.debug(f'Using {subclass.__name__} for {value}')
                    return subclass
        raise SessionError(
            f'Could not find an appropriate Session subclass for {args} {kwargs}'
        )

    def __lt__(self, other: Session) -> bool:
        if not hasattr(other, 'date'):
            return NotImplemented
        return self.date < other.date

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (int, str, Session)):
            return NotImplemented
        if isinstance(other, Session):
            return self.id == other.id
        return (
            str(self) == str(other)
            or str(self.id) == str(other)
            or self.folder == other
        )

    def __hash__(self) -> int:
        return hash(self.id) ^ hash(self.__class__.__name__)

    def __str__(self) -> str:
        return self.folder

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.folder!r})'

    id: int | str
    """Unique identifier for the session, e.g. lims ecephys session ID"""
    folder: str
    """Folder name for the session, e.g. '1116941914_576323_20210721'"""
    project: str | Project
    """Project name for the session, e.g. 'NeuropixelVisualBehavior'"""
    rig: Optional[str | np_config.Rig] = None
    """Rig ID, e.g. 'NP.0'"""
    lims: Optional[dict | LIMS2SessionInfo] = None
    mtrain: Optional[mtrain.MTrain] = None
    foraging_id: Optional[str] = None

    @staticmethod
    def get_folder(path: str | int | PathLike) -> str:
        """Extract the session folder from a path or session ID"""
        return NotImplemented
    
    @classmethod
    def new(cls, *args, **kwargs) -> Self:
        """Generate a new session (should create a folder and assign to npexp path)"""
        return NotImplemented
    
    @property
    def date(self) -> datetime.date:
        d = self.folder.split('_')[2]
        date = datetime.date(
            year=int(d[:4]), month=int(d[4:6]), day=int(d[6:])
        )
        return date

    @property
    def npexp_path(self) -> pathlib.Path:
        """np-exp root / folder (may not exist)"""
        return NPEXP_ROOT / self.folder
    
    @property
    def user(self) -> str | User | None:
        """Operator for the session"""
        if hasattr(self, '_user'):
            return self._user
    
    @property
    def mouse(self) -> str | Mouse:
        if not hasattr(self, '_mouse'):
            self._mouse = Mouse(self.folder.split('_')[1])
        return self._mouse

    is_ecephys: Optional[bool] = None
    """Whether the session is an ecephys session (None if not sure)"""

    is_hab: Optional[bool] = None
    """Whether the session is a hab session (None if not sure)"""

    @property
    def is_ecephys_session(self) -> bool | None:
        """Returns `is_ecephys`, for backwards compatibility"""
        return self.is_ecephys
    
    @property
    def start(self) -> datetime.datetime:
        """Session start time - defaults to start of day on the session date"""
        return datetime.datetime(*self.date.timetuple()[:5])

    @property
    def end(self) -> datetime.datetime:
        """Session end time - defaults to end of day on the session date"""
        return (
            datetime.datetime(*self.date.timetuple()[:5])
            + datetime.timedelta(days=1)
            - datetime.timedelta(seconds=1)
        )

    @property
    def probes_inserted(
        self,
    ) -> Optional[tuple[Literal['A', 'B', 'C', 'D', 'E', 'F'], ...]]:
        """None if no information has been set"""
        with contextlib.suppress(AttributeError):
            return self._probes
        return None

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
        self._probes = tuple(p for p in inserted)


    @property
    def sync(self) -> pathlib.Path | None: 
        files = tuple(
            itertools.chain(
                self.npexp_path.glob('*.sync'),
                self.npexp_path.glob(f'{self.date:%Y%m%d}T*.h5'),
            ),
        )
        if len(files) > 1:
            raise FileNotFoundError(
                f'Multiple sync files found: {files}'
            )
        return None if not files else files[0]
    
    @property
    def datajoint_path(self) -> pathlib.Path | None:
        path = paths.DATAJOINT_ROOT / 'ks_paramset_idx_1' / str(self)
        if path.exists():
            return path
        
    @cached_property
    def metrics_csv(self) -> tuple[pathlib.Path, ...]:
        for path in (
            self.npexp_path,
            self.datajoint_path,
            ):
            if path is None:
                continue
            csvs = tuple(path.rglob('metrics.csv'))
            if csvs:
                return csvs
        return ()
        
    @cached_property
    def probe_letter_to_metrics_csv_path(self) -> dict[str, pathlib.Path]:
        csv_paths = self.metrics_csv
        if not csv_paths:
            return {}

        def letter(x):
            return re.findall('(?<=_probe)[A-F]', str(x))

        probe_letters = [_[-1] for _ in map(letter, csv_paths) if _]
        if probe_letters:
            return dict(zip(probe_letters, csv_paths))
        return {}


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
