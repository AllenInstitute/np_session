from __future__ import annotations

import contextlib
import itertools
import pathlib
import shutil
from typing import Generator, Optional

import np_logging
from typing_extensions import Literal

from np_session.components.info import Mouse, Project, Projects, User
from np_session.components.paths import *
from np_session.components.platform_json import *
from np_session.databases import lims2 as lims
from np_session.utils import *
from np_session.exceptions import SessionError, FilepathIsDirError
from np_session.session import Session

logger = np_logging.getLogger(__name__)


def generate_session(
    mouse: str | int | Mouse,
    user: str | User,
    session_type: Literal['ephys', 'hab', 'behavior'] = 'ephys',
) -> Session:
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
    session = Session(lims_session)
    # assign instances with data already fetched from lims:
    session._mouse = mouse
    session._user = user
    return session


def sessions(
    project: Optional[str | Projects] = None,
    root: str | pathlib.Path = NPEXP_ROOT,
    session_type: Literal['ephys', 'hab', 'behavior'] = 'ephys',
) -> Generator[Session, None, None]:
    """Find Session folders in a directory.

    - `project` is the acronym used by the NP-ops team for the umbrella project:
        'DR', 'GLO', 'VAR', 'ILLUSION', 'TTN'
    - a `Projects` enum can also be used directly
    """
    root = pathlib.Path(root)

    if isinstance(project, str):
        project = getattr(Projects, project)

    for path in root.iterdir():
        if not path.is_dir():
            continue
        try:
            session = Session(path)
        except (SessionError, FilepathIsDirError):
            continue

        if (session_type == 'behavior') and (
            session.is_ecephys in (True, None) or session.is_hab
        ):   # watch out: is_ecephys is None if unsure. assumed to be ecephys here
            continue

        if (session_type != 'hab') and (
            session.is_hab
        ):   # watch out: is_hab is None if unsure
            continue

        if project and session.project not in project.value:
            continue

        if session.is_ecephys is None:
            logger.debug(
                "Unsure if %s is an ecephys or behavior session on lims, but it's included in results",
                session,
            )
        if session.is_hab is None:
            logger.debug(
                "Unsure if %s is a hab or ephys session, but it's included in results",
                session,
            )

        yield session


def cleanup_npexp():
    """Remove empty dirs, 366122 dirs, move habs"""

    def remove_non_empty_dir(path: pathlib.Path):
        shutil.rmtree(path, ignore_errors=True)
        if not path.exists():
            logger.info('Removed %s', path.name)

    for _ in itertools.chain(
        NPEXP_ROOT.iterdir(), (NPEXP_ROOT / 'habituation').iterdir()
    ):
        if not _.is_dir():
            continue
        with contextlib.suppress(Exception):
            _.rmdir()
            logger.info('Removed empty dir %s', _.name)
            continue
        if '_366122_' in _.name:
            remove_non_empty_dir(_)
            continue
        contents = tuple(_.iterdir())
        if (
            len(contents) == 1
            and contents[0].suffix == '.json'
            and 'platform' in contents[0].name
        ):
            remove_non_empty_dir(_)
            continue
        try:
            session = Session(_)
        except SessionError:
            continue
        if session.is_hab and _.parent == NPEXP_ROOT:
            try:
                _.replace(NPEXP_ROOT / 'habituation' / 'backup' / _.name)
            except OSError as exc:
                logger.error('Moving hab failed: %r', exc)
            else:
                logger.info('Moved %s to habituation/backup', _.name)


def latest_session(
    project: str | Project | Projects,
    session_type: Literal['ephys', 'hab', 'behavior'] = 'ephys',
) -> Session | None:

    if isinstance(project, str):
        if project.upper() in Projects.__members__:
            project = Projects[project.upper()]

    session: str | int | None = None
    if isinstance(project, Projects):
        session = project.latest_session(session_type)
    if isinstance(project, Project):
        session = project.state.get(f'latest_{session_type}')
    if session is None:
        logger.info('No latest session found for %s', project)
        return None

    return Session(session)
