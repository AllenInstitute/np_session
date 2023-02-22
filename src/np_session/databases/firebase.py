from collections.abc import MutableMapping
from typing import ClassVar, Iterator
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import np_logging
import pathlib
import json
import doctest

AcceptedType = bool
class State(MutableMapping):
    """Get and set session state in Firebase via a dict interface.
    - dict interface provides `keys`, `get`, `setdefault`, `pop`, etc.
    - accepted value types are str, int, float, bool, None

    >>> test_lims_id = 123456
    >>> state = State(test_lims_id)
    >>> state.data[state.id]['alignment']
    False
    >>> another_id = 1234567
    >>> value: dict = {'aligment': True, 'annotated': True, 'qc_state': True}
    >>> state = State(another_id)
    >>> state.insertDocument(value)
    >>> state.data[state.id]['qc_state']
    True
    >>> state.updateState('qc_state', False)
    >>> state.data[state.id]['qc_state']
    False
    >>> state.deleteState('qc_state')
    >>> len(list(state.data[state.id].keys()))
    2
    """
    db: ClassVar
    def __init__(self, lims_session_id: int) -> None:
        self.id = str(lims_session_id)

        try:
            _ = self.db
        except AttributeError:
            self.__class__.connect()

    @classmethod
    def connect(cls) -> None:
        key_path = pathlib.Path('//allen/scratch/aibstemp/arjun.sridhar/db_key.json')
        cred = credentials.Certificate(key_path)
        cls.app = firebase_admin.initialize_app(cred)
        cls.db = firestore.client()
        #cls.ref = cls.db.reference('/session_state') # root user, can create users and add them also if needed

    @property
    def session_doc(self):
        """
        returns document snapshot
        """

        return self.db.collection(u'session_state').document(str(self.id))

    @property
    def data(self) -> dict[str, AcceptedType]:
        session_dict = {}
        session_dict.setdefault(self.session_doc.id, self.session_doc.get().to_dict())

        return session_dict

    def __getitem__(self, key: str) -> dict:
        return self.data[key]

    def __delitem__(self, key: str) -> None:
        """
        deletes field from database for session
        """
        self.session_doc.update({key: firestore.DELETE_FIELD})

    def __len__(self) -> int:
        return len(self.data)

    def __setitem__(self, key: str, value: AcceptedType) -> None:
        """
        updates the database with the key value item
        """
        self.session_doc.update({key: value})

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)
    
    def setState(self, key: str, value: AcceptedType) -> None: 
        self.__setitem__(key, value)

    def insertDocument(self, value: dict) -> None:
        """
        inserts a document into database for the session
        """
        self.session_doc.set(value)

    def getState(self, key: str) -> AcceptedType:
        """
        wrapper that retrieves the state of the key for the session
        """

        return self.__getitem__(key)

    def updateState(self, key: str, state: AcceptedType) -> None:
        """
        wrapper that updates the state of the key for the session
        """
        self.__setitem__(key, state)

    def deleteState(self, key: str) -> None:
        """
        wrapper that removes the state field for the session
        """
        self.__delitem__(key)

if __name__ == '__main__':
    doctest.testmod()

