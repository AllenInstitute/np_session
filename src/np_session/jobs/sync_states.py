import np_logging

from np_session.databases.firebase_state import State as Firebase
from np_session.databases.redis_state import State as Redis

logger = np_logging.getLogger(__name__)


def sync_redis_to_firebase() -> None:
    """Sync Redis state to Firebase state."""
    for id in Redis.db.keys():
        id = id.decode()   # Redis keys are bytes
        Firebase(id).update(Redis(id))
        logger.debug(f'Synced {id} from Redis to Firebase.')


if __name__ == '__main__':
    Firebase.connect()
    Redis.connect()
    sync_redis_to_firebase()
