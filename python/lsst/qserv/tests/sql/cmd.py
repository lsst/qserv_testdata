
from __future__ import absolute_import, division, print_function

import logging
import subprocess
import time

from lsst.qserv.admin import commons
from . import const


# TODO: replace all SQL by SQLConnection
class Cmd(object):
    """
    Run mysql client

    Parameters
    ----------
    config: `dict`
        Keys are configuration section names, values are dicts.
    mode: `int`
        One of const.MYSQL_PROXY, const.MYSQL_SOCK, const.MYSQL_NET.
    database: `str`
        Default database name.
    """

    def __init__(self, config, mode, database):
        self.config = config

        self.logger = logging.getLogger(__name__)
        self.logger.debug("SQL cmd creation")

        self._mysql_cmd = ["mysql"]

        if mode == const.MYSQL_PROXY:
            self._addQservCmdParams()
        elif mode == const.MYSQL_SOCK:
            self._addMySQLSockCmdParams()
        elif mode == const.MYSQL_NET:
            self._addMySQLNetCmdParams()

        self._mysql_cmd.append("--batch")
        if database is not None:
            self._mysql_cmd.append(database)

        self.logger.debug("SQLCmd._mysql_cmd %s", self._mysql_cmd)

    def _addQservCmdParams(self):
        self._mysql_cmd.append("--host=%s" % self.config['qserv']['master'])
        self._mysql_cmd.append("--port=%s" % self.config['mysql_proxy']['port'])
        self._mysql_cmd.append("--user=%s" % self.config['qserv']['user'])

    def _addQservSockCmdParams(self):
        """User root should not be used for data loading, only for database
        creation and to give rights. Use qsmaster user instead.
        """
        self._mysql_cmd.append("--socket=%s" % self.config['mysqld']['socket'])
        self._mysql_cmd.append("--user=%s" % self.config['qserv']['user'])

    def _addMySQLSockCmdParams(self):
        self._mysql_cmd.append("--socket=%s" % self.config['mysqld']['socket'])
        self._mysql_cmd.append("--user=%s" % self.config['mysqld']['user'])
        self._mysql_cmd.append("--password=%s" % self.config['mysqld']['pass'])

    def _addMySQLNetCmdParams(self):
        self._mysql_cmd.append("--host=%s" % self.config['qserv']['master'])
        self._mysql_cmd.append("--port=%s" % self.config['mysqld']['port'])
        self._mysql_cmd.append("--user=%s" % self.config['mysqld']['user'])
        self._mysql_cmd.append("--password=%s" % self.config['mysqld']['pass'])

    def execute(self, query, output=None, column_names=True, async_timeout=0, output_err=None):
        """Execute query and send result to specified output.

        Parameters
        ----------
        query : `str`
            Query string.
        output : object, optional
            Either file object or file name, by default output goes to stdout.
        column_names : boolean, optional
            If `False` then column names are not printed.
        async_timeout : int, optional
            If >0 then query will run disconnected, its value gives a timeout
            in seconds to wait for query completion.
        output_err: object, optional
            Either file object or file name to write error output to.
        """
        self.logger.debug("SQLCmd.execute:  %s", query)
        if async_timeout > 0:

            # run SUBMIT command and wait until it finishes
            commandLine = self._mysql_cmd[:]
            commandLine.append('--skip-column-names')
            commandLine += ['-e', "SUBMIT " + query]
            self.logger.debug("SQLCmd.execute running SUBMIT query")
            try:
                data = subprocess.check_output(commandLine)
            except subprocess.CalledProcessError as exc:
                self.logger.error("SUBMIT failed: %s", exc)
                return

            # read query ID
            try:
                qid = int(data.split()[0])
                self.logger.debug("SQLCmd.execute query ID = %s", qid)
            except Exception:
                raise RuntimeError("Failed to read query ID from SUBMIT: %s",
                                   data)

            # wait until query completes
            query = "SELECT STATE FROM INFORMATION_SCHEMA.PROCESSLIST "\
                    "WHERE ID = {}".format(qid)
            commandLine = self._mysql_cmd[:]
            commandLine.append('--skip-column-names')
            commandLine += ['-e', query]
            self.logger.debug("SQLCmd.execute waiting for query to complete")
            end_time = time.time() + async_timeout
            while time.time() < end_time:
                try:
                    data = subprocess.check_output(commandLine)
                except subprocess.CalledProcessError as exc:
                    self.logger.error("Async status query failed: %s", exc)
                    return
                status = data.strip()
                self.logger.debug("SQLCmd.execute query status = %s", status)
                if status == b"COMPLETED":
                    break
            else:
                raise RuntimeError("Timeout while waiting for detached query")

            # OK, we are here, means query completed, to retrieve its result
            # we need different query
            query = "SELECT * from qserv_result({})".format(qid)

        commandLine = self._mysql_cmd[:]
        if not column_names:
            commandLine.append('--skip-column-names')
        commandLine += ['-e', query]
        commons.run_command(commandLine, stdout=output, stderr=output_err)
