from __future__ import annotations

import doctest
import pathlib
from collections.abc import MutableMapping
from typing import ClassVar, Iterator, Union

import firebase_admin
import firebase_admin.firestore as firestore

AcceptedType = Union[str, int, float, bool, list, None]


class State(MutableMapping):
    """Get and set session state in Firebase via a dict interface.

    - dict interface provides `keys`, `get`, `setdefault`, `pop`, etc.
    - accepted value types are str, int, float, bool, None

    >>> test_id = 123456
    >>> state = State(test_id)
    >>> state['new'] = 1.0
    >>> state['new']
    1.0
    >>> state['new'] = 'new'
    >>> state['new']
    'new'
    >>> all('new' in _ for _ in (state, state.keys(), state.values()))
    True
    >>> state.setdefault('new', True)
    'new'
    >>> state.pop('new')
    'new'
    >>> del state['new']
    >>> state.get('new') is None
    True
    """

    db: ClassVar

    def __init__(self, id: int | str) -> None:
        self.id = str(id)
        try:
            _ = self.db
        except AttributeError:
            self.__class__.connect()

    def __repr__(self) -> str:
        return repr(self.session_doc.get().to_dict())

    @classmethod
    def connect(cls) -> None:
        key_path = pathlib.Path(
            '//allen/scratch/aibstemp/arjun.sridhar/db_key.json'
        )
        cred = firebase_admin.credentials.Certificate(key_path)
        cls.app = firebase_admin.initialize_app(cred)
        cls.db = firestore.client().collection('session_state')
        # cls.ref = cls.db.reference('/session_state') # root user, can create users and add them also if needed

    @property
    def session_doc(self):
        """
        returns document snapshot
        """
        doc = self.db.document(str(self.id))
        if doc.get().to_dict() is None:
            doc.set({})
        return doc

    def __getitem__(self, key: str) -> AcceptedType:
        return self.session_doc.get().to_dict()[key]

    def __delitem__(self, key: str) -> None:
        """
        deletes field from database for session
        """
        self.session_doc.update({key: firestore.DELETE_FIELD})

    def __len__(self) -> int:
        return len(self.session_doc.get().to_dict())

    def __setitem__(self, key: str, value: AcceptedType) -> None:
        """
        updates the database with the key value item
        """
        return self.session_doc.update({key: value})

    def __iter__(self) -> Iterator[str]:
        return iter(self.session_doc.get().to_dict())


if __name__ == '__main__':
    doctest.testmod(verbose=True)
