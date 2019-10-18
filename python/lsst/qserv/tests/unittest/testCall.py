# LSST Data Management System
# Copyright 2019 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.


import tempfile
import unittest

from lsst.qserv.admin import commons
from lsst.qserv.tests.sql import cmd, const



class TestCall(unittest.TestCase):

    qservServer = ''

    def test_call(self):
        self.config = commons.read_user_config()
        if self.qservServer:
            self.config['qserv']['master'] = self.qservServer

        sqlInterface = cmd.Cmd(config=self.config, 
                               mode=const.MYSQL_PROXY, 
                               database='qservTest_case01_qserv')

        queryText = "CALL QSERV_MANAGER('foo')"
        expected = ['response\n', 'foo\n']
        with tempfile.NamedTemporaryFile('r') as f:
            sqlInterface.execute(queryText, f.name)
            f.seek(0)
            self.assertEqual(expected, f.readlines())


def suite(qserv_server=""):
    TestCall.qservServer = qserv_server
    return unittest.TestLoader().loadTestsFromTestCase(TestCall)


if __name__ == '__main__':
    unittest.main()
