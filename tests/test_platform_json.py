import json
import os
import pathlib
import tempfile
import time


os.environ["USE_TEST_RIG"] = "0"
os.environ["AIBS_RIG_ID"] = "NP.0"

import np_logging
logger = np_logging.getLogger()
logger.setLevel(10)

import pytest
from np_session import Session, PlatformJson


session = Session('1246096278_366122_20230209')

@pytest.fixture
def p(tmp_path):
    return PlatformJson(path=tmp_path / session.folder)

def test_filename(p):
    assert p.path.name.endswith(PlatformJson.suffix)
    assert session.folder in p.path.name

def test_path(p, tmp_path):
    assert tmp_path in p.path.parents

def test_datetime(p):
    original = p.workflow_start_time = '20230214133000'
    assert p.model_dump()['workflow_start_time'] == original
    
def test_write_read_update(p):
    p.workflow_start_time = '20230214133000'
    initial_time = p.workflow_start_time
    time.sleep(1)
    p.workflow_start_time = '20230214133001'
    updated_time = p.workflow_start_time
    assert updated_time > initial_time
    p = PlatformJson(path=p.path)
    assert p.workflow_start_time != initial_time
    assert p.workflow_start_time == updated_time
    

    
if __name__ == '__main__':
    pytest.main([__file__])