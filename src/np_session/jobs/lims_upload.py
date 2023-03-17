import pathlib

import np_logging

from np_session.session import Session
from np_session.components.paths import INCOMING_ROOT as DEFAULT_INCOMING_ROOT

logger = np_logging.getLogger(__name__)


def write_trigger_file(
    session: Session, 
    incoming_dir: pathlib.Path = DEFAULT_INCOMING_ROOT,
    trigger_dir: pathlib.Path = DEFAULT_INCOMING_ROOT / "trigger",
    ) -> None:
    """Write a trigger file to initiate ecephys session data upload to lims.
    
    - designated "incoming" folders have a `trigger` dir which is scanned periodically for trigger files
    - a trigger file provides:
        - a lims session ID 
        - a path to an "incoming" folder where new session data is located, ready for
          upload
            - this path is typically the parent of the trigger dir, where lims has
              read/write access for deleting session data after upload, but it can be
              anywhere on //allen
        - once the trigger file is detected, lims searches for a file in the incoming
          dir named '*platform*.json', which should contain a `files` dict
    """
    if not incoming_dir.exists():
        logger.warning("Incoming dir doesn't exist or isn't accessible - lims upload job will fail when triggered: %s", incoming_dir)
    elif not incoming_dir.match(f"*{session.id}*platform*.json"):
        logger.warning("No platform json found for %s in incoming dir - lims upload job will fail when triggered: %s", session.id, incoming_dir)
        
    trigger_file = pathlib.Path(trigger_dir / f"{session.id}.ecp")
    trigger_file.touch()    
    # don't mkdir for trigger_dir or parents 
    # - doesn't make sense to create, since it's a dir lims needs to know about and
    #   be set up to monitor
    # - if it doesn't exist or is badly specified, the file
    #   operation should raise the appropriate error 
    
    contents = (
        f"sessionid: {session.id}\n"
        f"location: '{incoming_dir.as_posix()}'"
    )
    trigger_file.write_text(contents)
        
    logger.info("Trigger file written for %s in %s:\n%s", session, trigger_file.parent, trigger_file.read_text())