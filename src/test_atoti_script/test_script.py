import unittest

import atoti as tt
from atoti_script.init import start_application, CUBE_NAME, main


class TestScript(unittest.TestCase):
    def test_script(self):
        with tt.Session.start() as session:
            start_application(session)

            cube = session.cubes[CUBE_NAME]
            cube = session.cubes[CUBE_NAME]
            self.assertEqual(
                cube.query(cube.measures["Cumulative amount"])["Cumulative amount"][0],
                961_463,
            )
