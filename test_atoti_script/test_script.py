import unittest

import atoti as tt
from atoti_script.init import start_application, CUBE_NAME


class TestScript(unittest.TestCase):
    def test_script(self):
        with tt.Session() as session:
            start_application(session)

            cube = session.cubes[CUBE_NAME]
            self.assertEqual(
                cube.query(cube.measures["Cumulative amount"])["Cumulative amount"][0],
                961_463,
            )
