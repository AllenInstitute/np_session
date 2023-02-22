"""
Redis connection for persisting np_session state.


"""
from __future__ import annotations

import collections.abc
import pathlib
from typing import ClassVar

import np_logging
import redis

logger = np_logging.getLogger(__name__)

class State(collections.abc.MutableMapping):
    """Get and set session state in Redis via a dict interface.
    
    >>> test_lims_id = 00000000
    >>> state = State(test_lims_id)
    >>> state['test'] = True
    >>> state['test']
    True
    >>> del state['test']
    >>> state.get('test') == None
    True
    """
    
    db: ClassVar[redis.Redis]
    
    ssl_ca_certs: str = str(pathlib.Path("//allen/scratch/aibstemp/arjun.sridhar/redis_ca.pem"))
    
    def __init__(self, lims_session_id: int) -> None:
        self.name = str(lims_session_id)
        try:
            _ = self.db
        except AttributeError:
            self.__class__.connect()
    
    @classmethod
    def connect(cls) -> None:
        cls.db = redis.Redis(
            host='redis-11877.c1.us-west-2-2.ec2.cloud.redislabs.com', 
            port=11877,
            )
        logger.debug("Connected to Redis database: %s", cls.db)
    
    @property
    def data(self) -> dict:
       return {decode(k): decode(v) for k,v in self.db.hgetall(self.name).items()}
        
    def __getitem__(self, key):
        return decode(self.db.hget(self.name, key))
    
    def __setitem__(self, key, value):
        self.db.hset(self.name, str(encode(key)), encode(value))
        
    def __delitem__(self, key): 
        self.db.hdel(self.name, str(encode(key)))
        
    def __iter__(self):
        return iter(self.data)
            
    def __len__(self):
        return len(self.data)

def encode(value: str | int | bool | None) -> int | str:
    """Redis can't store bools: convert to something compatible before entering."""
    if value is True:
        return 'True'
    if value is False:
        return 'False'
    if value is None:
        return 'None'
    return value

def decode(value: bytes | None) -> str | int | bool | None:
    """Redis stores everything as bytes: convert back to our original python datatype."""
    if value is None:
        return None
    decoded: str = value.decode()
    if decoded.isnumeric():
        return int(decoded)
    if decoded.capitalize() == 'True':
        return True
    if decoded.capitalize() == 'False':
        return False
    if decoded.capitalize() == 'None':
        return None
    return decoded