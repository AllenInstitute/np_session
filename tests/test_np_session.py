import doctest

import np_session
from np_session import *

doctest.testmod(np_session.session)

Session('1233182025_649324_20221215').data_dict['EcephysRigSync']
Session('1233182025_649324_20221215').mtrain
Session('1233182025_649324_20221215').project.lims