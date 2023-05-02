import doctest

from np_session import *


Session('1233182025_649324_20221215').data_dict['EcephysRigSync']
Session('1233182025_649324_20221215').mtrain
Session('1233182025_649324_20221215').project.lims


_ = Session('1200879339_634837_20220825')

def test_readme():
    doctest.testfile("README.md", module_relative=False, verbose=True)

def test_session_init_fake_path():
    a1 = DRPilotSession('c:/DRpilot_366122_20220822_surface-image1-left.png')
    a2 = Session('c:/DRpilot_366122_20220822_surface-image1-left.png')
    b1 = Session('c:/1116941914_576323_20210721_surface-image1-left.png')
    b2 = PipelineSession('c:/1116941914_576323_20210721_surface-image1-left.png')
    assert a1 == a2
    assert b1 == b2

def test_DR_pilot_real():
    s = DRPilotSession('//allen/programs/mindscope/workgroups/np-exp/PilotEphys/Task 2 pilot/DRpilot_644866_20230207')
    assert s.npexp_path.exists()