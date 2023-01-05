import doctest
import unittest

from np_session import *

doctest.testmod(np_session)


class TestPathFuncs(unittest.TestCase):
    fictional_folder = "1234567890_366122_20220618"
    actual_folder = ""

session = Session('c:/1116941914_surface-image1-left.png')
session.data_dict
session.mtrain
session.mouse.project_name
