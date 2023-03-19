"""
Redis connection for persisting np_session state.


"""
from __future__ import annotations

import collections.abc
import contextlib
import doctest
import pathlib
from typing import ClassVar, Iterator, Union

import np_logging
import redis

logger = np_logging.getLogger(__name__)

password_file = pathlib.Path(
        "//allen/scratch/aibstemp/arjun.sridhar/redis_db.txt"
)

# AcceptedType will be coerced to RedisType before being stored in Redis:
RedisType = Union[str, int, float]
"""Can be stored in Redis directly, or returned from Redis."""
AcceptedType = Union[RedisType, bool, None]
"""Can be stored in Redis after using `encode(value: AcceptedType)`."""


class State(collections.abc.MutableMapping):
    """Get and set session state in Redis via a dict interface.

    - dict interface provides `keys`, `get`, `setdefault`, `pop`, etc.
    - accepted value types are str, int, float, bool, None
    
    >>> test_id = 0
    >>> state = State(test_id)
    >>> state['test'] = 1.0
    >>> state['test']
    1.0
    >>> state['test'] = 'test'
    >>> state['test']
    'test'
    >>> all('test' in _ for _ in (state, state.keys(), state.values()))
    True
    >>> state.setdefault('test', True)
    'test'
    >>> state.pop('test')
    'test'
    >>> del state['test']
    >>> state.get('test') is None
    True
    """

    db: ClassVar[redis.Redis]

    def __init__(self, id: int | str) -> None:
        self.name = str(id)
        try:
            _ = self.db
        except AttributeError:
            self.__class__.connect()

    def __repr__(self) -> str:
        return self.data.__repr__()
    
    @classmethod
    def connect(cls) -> None:
        password = password_file.read_text().strip()
        cls.db = redis.Redis(
            host="redis-11877.c1.us-west-2-2.ec2.cloud.redislabs.com",
            port=11877,
            password=password,
        )
        if cls.db.ping():
            logger.debug("Connected to Redis database: %s", cls.db)
        else:
            logger.error("Failed to connect to Redis database")
            
    @property
    def data(self) -> dict[str, AcceptedType]:
        return {k.decode(): decode(v) for k, v in self.db.hgetall(self.name).items()}

    def __getitem__(self, key: str) -> AcceptedType:
        _ = decode(self.db.hget(self.name, key))
        if _ is None:
            raise KeyError(f"{key!r} not found in Redis db entry {self!r}")
        return _
    
    def __setitem__(self, key: str, value: AcceptedType) -> None:
        self.db.hset(self.name, key, encode(value))

    def __delitem__(self, key: str) -> None:
        self.db.hdel(self.name, key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

def encode(value: AcceptedType) -> RedisType:
    """Redis can't store bools: convert to something compatible before entering."""
    if value in (True, False, None):
        return str(value)
    return value

def decode(value: bytes | None) -> AcceptedType:
    """Redis stores everything as bytes: convert back to our original python datatype."""
    if value is None:
        return None
    decoded_value: str = value.decode()
    if decoded_value.isnumeric():
        return int(decoded_value)
    with contextlib.suppress(ValueError):
        return float(decoded_value)
    if decoded_value.capitalize() in (str(_) for _ in (True, False, None)):
        return eval(decoded_value.capitalize())
    return decoded_value


if __name__ == "__main__":
    doctest.testmod(verbose=True)