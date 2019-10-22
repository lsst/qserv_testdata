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


import re
import tempfile
import unittest

from lsst.qserv.admin import commons
from lsst.qserv.tests.sql import cmd, const



class TestInvlidColumn(unittest.TestCase):
    """This is a suite of tests to test when invalid columns are used that it is caught by Qserv and that
    an expected error message is returned to the user.
    """

    qservServer = ''


    @staticmethod
    def replace(in_string, find_regex, replacement_string):
        """Search a string for a regex pattern and replace the pattern with a given replacement.

        Parameters
        ----------
        in_string : string
            The string to search in.
        find_regex : string
            The regex pattern to search for in the string.
        replacement_string : string
            The replacement string for any found instances of the regex pattern.

        Returns
        -------
        string
            The original string after replacements have been made.
        """
        compiled = re.compile(find_regex)
        while True:
            match = compiled.search(in_string)
            if (match):
                in_string = in_string.replace(match.group(), replacement_string)
            else:
                break
        return in_string


    def test_invalid_order_by_column(self):
        """Tests that a query with an ORDER BY column that does not exist will return the expected error
        message.
        """
        self.config = commons.read_user_config()
        if self.qservServer:
            self.config['qserv']['master'] = self.qservServer

        sqlInterface = cmd.Cmd(config=self.config,
                               mode=const.MYSQL_PROXY,
                               database='qservTest_case01_qserv')

        queryText = "SELECT * FROM Object ORDER BY foo LIMIT 1"
        expected = "ERROR 4005 (Proxy) at line 1: Exception in call to czar method: QI=123: Unknown column 'foo' in 'order clause'\n"
        expected = TestCall.replace(expected, 'QI=[0-9]+', 'QI=nnn')
        with tempfile.NamedTemporaryFile('r') as output:
            with tempfile.NamedTemporaryFile('r') as error:
                sqlInterface.execute(queryText, output=output.name, output_err=error.name)
                output.seek(0)
                errmsg = error.readlines()
                self.assertEqual(len(errmsg), 1)
                errmsg = TestCall.replace(errmsg[0], 'QI=[0-9]+', 'QI=nnn')
                self.assertEqual(expected, errmsg)


def suite(qserv_server=""):
    TestCall.qservServer = qserv_server
    return unittest.TestLoader().loadTestsFromTestCase(TestCall)


if __name__ == '__main__':
    unittest.main()
