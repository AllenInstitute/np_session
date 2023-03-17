from __future__ import annotations
import contextlib

import pathlib
from typing import Optional

from typing_extensions import Literal
from backports.cached_property import cached_property

import np_config
import np_logging
import pydantic

import np_session.session
import np_session.components.info

logger = np_logging.getLogger(__name__)

SESSION_TYPES = ('D0', 'D1', 'D2', 'hab')
MANIFESTS: dict[str, dict] = (
    np_config.from_zk(f'projects/np_session/manifests')
)
        
class Manifest:
    # class Config:
    #     # validate_assignment = True # coerce types on assignment
    #     # extra = 'allow' # 'forbid' = properties must be defined in the model
    #     fields = {'names': {'exclude': True}, 'globs': {'exclude': True},  'types': {'exclude': True}}
    #     arbitrary_types_allowed = True
        
    session: Optional[np_session.session.Session] = None
    """`np_session.Session` object"""
    project: Optional[str] = None
    """Umbrella project abbrv/acronym (DR, TTN, GLO)."""
    session_type: Optional[Literal['D0', 'D1', 'D2', 'hab']] = None
    """Type of manifest for a specific session upload, e.g. `D1` for D1 upload."""
    path: Optional[str | pathlib.Path] = None
    """Session folder path, to be used with `globs`."""
    
    names: tuple[str, ...]
    """Descriptive lims names for each file in manifest, in order, e.g. `synchronization_data`."""
    globs: tuple[str, ...]
    """Globbable filename patterns relative to session folder for each file in manifest, in order, e.g. `.sync`."""""
    types: tuple[Literal['filename', 'directory_name'], ...]
    """`filename` or `directory_name` for each file in manifest, in order."""
    
    def __init__(self, *args, **kwargs) -> None:      
        
        self.__dict__.update(kwargs)
        
        self.parse_input_args(*args, **kwargs)
        
        if self.session:
            self.assign_props_from_session()
        elif self.session_type is None:
            self.session_type = 'D1'
            
        self.fetch_from_zk()
    
    @property
    def files(self) -> dict[str, dict[str, str]]:
        """Upload manifest for platform json: `{name: {type: session + glob}}, ...}`"""
        return {k: {v: f'{self.session.folder if self.session else ""}{g.strip("*")}'} for k, v, g in zip(self.names, self.types, self.globs)}
    
    @cached_property
    def paths(self) -> tuple[pathlib.Path | None, ...]:
        """First session npexp folder + glob match, if any exist, for each file in manifest,
        in order. `None` if no matches in filesystem.
        
        If `self.path` is set, use that as the session folder path.
        
        See `missing` for files that do not exist.
        """
        if self.path:
            path = pathlib.Path(self.path)
        elif self.session:
            path = self.session.npexp_path
        else:
            raise ValueError(f'Must provide either `path` or `session` as {self.__class__} property to return paths.')
        paths = []
        for _ in self.globs:
            hits = tuple(path.glob(_))
            if len(hits) == 0:
                logger.warning(f'No files found for glob: {path / _}')
            if len(hits) > 1:
                logger.warning(f'Multiple files found for glob: {path / _} - {hits} - using first.')
            
            paths.append(hits[0] if hits else None)
        
        return tuple(paths)
    
    @property
    def missing(self) -> tuple[str, ...]:
        return tuple(n for n, p in zip(self.names, self.paths) if p is None)
    
    
    def get_sorted_data(self) -> tuple[pathlib.Path | None, ...]:
        self._names_sorted_data = []
        self._paths_sorted_data = []
        self._globs_sorted_data = []
        for probe in 'ABCDEF':
            for name, glob in MANIFESTS['_name_glob']['sorted_data'].items():
                probe_glob =f'*_probe{probe}{glob}'
                self._globs_sorted_data.append(probe_glob)
                self._names_sorted_data.append(f'{name}_probe{probe}')
                hits = tuple(self.session.npexp_path.glob(probe_glob))
                if len(hits) == 0:
                    logger.warning(f'No files found for glob: {self.session.npexp_path / probe_glob}')
                if len(hits) > 1:
                    logger.warning(f'Multiple files found for glob: {self.session.npexp_path / probe_glob} - {hits} - using first.')
                self._paths_sorted_data.append(hits[0] if hits else None)
        
    @property
    def names_sorted_data(self) -> tuple[str]:
        with contextlib.suppress(AttributeError):
            return tuple(self._names_sorted_data)
        self.get_sorted_data()
        return self.names_sorted_data
    
    @property    
    def paths_sorted_data(self) -> tuple[pathlib.Path | None]:
        with contextlib.suppress(AttributeError):
            return tuple(self._paths_sorted_data)
        self.get_sorted_data()
        return self.paths_sorted_data
    
    @property    
    def globs_sorted_data(self) -> tuple[pathlib.Path | None]:
        with contextlib.suppress(AttributeError):
            return tuple(self._globs_sorted_data)
        self.get_sorted_data()
        return self.globs_sorted_data
    
    @property
    def missing_sorted_data(self) -> tuple[str]:
        return tuple(n for n, p in zip(self.names_sorted_data, self.paths_sorted_data) if p is None)
    
    def parse_input_args(self, *args, **kwargs) -> None:
        for _ in (*args, *kwargs.values()):
            if isinstance(_, np_session.session.Session):
                self.session = _
                break
            elif isinstance(_, int):
                self.session = np_session.session.Session(_)
                break
        
        for _ in SESSION_TYPES:
            if _ in (*args, *kwargs.values()):
                self.session_type = _
                break
            
        for _ in (*args, *kwargs.values()):
            if _ in np_session.session.Projects.__members__:
                self.project = _
                break
    
    
    def assign_props_from_session(self) -> None:
        if self.project is None:
            self.project = self.session.project.parent.name
            logger.debug(f'No project provided, using {self.project} for {self.session}')
        if self.session_type is None:
            self.session_type = 'hab' if self.session.is_hab else 'D1'
            logger.debug(f'No session_type provided, using {self.session_type} for {self.session}')
        
    def fetch_from_zk(self) -> None:
        """Fetch names, file globs and file/dir types from zookeeper."""
        project = 'default' if self.project is None else self.project
        default = MANIFESTS[self.session_type]['default']
        if project not in MANIFESTS[self.session_type]:
            logger.debug(f'No manifest found for {project} in {self.session_type} manifests on ZK - using default.')
        name_glob: dict[str, str] = MANIFESTS[self.session_type].get(project, default)
        
        self.names, self.globs = tuple(name_glob.keys()), tuple(name_glob.values())
        
        name_type: dict[str, str] = MANIFESTS['_name_type']
        self.types = tuple(name_type[_] for _ in self.names)

    def __repr__(self) -> str:
        return repr(self.files)
    
if __name__ == "__main__":
    Manifest('TTN').files
    Manifest('TTN', 'D2').files
    Manifest(1254184444, 'D2').paths
    Manifest(1208053773, 'D1').missing_sorted_data