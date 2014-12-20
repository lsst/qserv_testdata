# Loads partition and load data set and then configure Qserv
# used by test procedure

import logging
import os
import sys
import tempfile

from lsst.qserv.admin import commons
from lsst.qserv.tests.sql import const, cmd, connection

class QservDataLoader():

    def __init__(self, config,
                 data_reader,
                 db_name,
                 out_dirname,
                 log_file_prefix='qserv-loader',
                 logging_level=logging.DEBUG):

        self.config = config
        self.dataReader = data_reader
        self.dataConfig = data_reader.dataConfig
        self._dbName = db_name

        self._out_dirname = out_dirname

        self.logger = logging.getLogger()
        self.sock_params = {
            'config': self.config,
            'mode': const.MYSQL_SOCK
            }

        self._sqlInterface = dict()

    def createLoadTable(self, table):
        """
        Create and load a table in Qserv
        """
        self._callLoader(table)

    def _callLoader(self, table):
        """
        Call Qserv loader
        """
        self.logger.info("Create, load partitioned table %s", table)

        tmp_dir = self.config['qserv']['tmp_dir']
        run_dir = self.config['qserv']['run_base_dir']
        loader_cmd = [
            'qserv-data-loader.py',
            '--verbose-all',
            '-vvv',
            '--config={0}'.format(os.path.join(self.dataConfig['input-dir'],
                                               "common.cfg")),
            '--css-remove',
            '--user={0}'.format(self.config['mysqld']['user']),
            '--password={0}'.format(self.config['mysqld']['pass']),
            '--socket={0}'.format(self.config['mysqld']['sock']),
            # TODO: load emptyChunk only for director table
            '--empty-chunks={0}'.format(os.path.join(run_dir,
                                                     "var", "lib", "qserv",
                                                     "empty_" +
                                                     self._dbName +
                                                     ".txt")),
            '--delete-tables']

        if table in self.dataConfig['partitioned-tables']:
            loader_cmd += [
                '--config={0}'.format(os.path.join(self.dataConfig['input-dir'],
                                                   table + ".cfg")),
                '--chunks-dir={0}'.format(os.path.join(tmp_dir,
                                                       "loader_chunks",
                                                       table))]
        else:
            loader_cmd += ['--skip-partition', '--one-table']

        loader_cmd += [
            self._dbName,
            table,
            self.dataReader.getSchemaFile(table),
            self.dataReader.getInputDataFile(table)]

        out = commons.run_command(loader_cmd)
        self.logger.info("Partitioned %s data loaded (stdout : %s)", table, out)

    def prepareDatabase(self):
        """
        Connect to MySQL via sock
        Create MySQL database
        Create MySQL command-line client
        Drop CSS database
        """

        self._sqlInterface['sock'] = connection.Connection(**self.sock_params)

        self.logger.info("Drop Qserv database from CSS: %s", self._dbName)

        self.logger.info("Drop and create Qserv database: %s", self._dbName)
        sql_instructions = [
            "DROP DATABASE IF EXISTS %s" % self._dbName,
            "CREATE DATABASE %s" % self._dbName,
            ("GRANT ALL ON {0}.* TO '{1}'@'localhost'"
             .format(self._dbName, self.config['qserv']['user'])),
            "USE {0}".format(self._dbName)
            ]

        for sql in sql_instructions:
            self._sqlInterface['sock'].execute(sql)

        cmd_connection_params = self.sock_params
        cmd_connection_params['database'] = self._dbName
        self._sqlInterface['cmd'] = cmd.Cmd(**cmd_connection_params)
        
        self.logger.info("Drop CSS database")
        self.dropCssDatabase()
        
    def dropCssDatabase(self):
        script = "qserv-admin.py"
        cmd = [script,
               "-c",
               "localhost:%s" % self.config['zookeeper']['port'],
               "-v",
               str(self.logger.getEffectiveLevel()),
               "-f",
               os.path.join(self.config['qserv']['log_dir'],
                            "qadm-%s.log" % self.dataConfig['data-name'])]
        
        with tempfile.NamedTemporaryFile('w+t') as f:
            f.write('DROP DATABASE {0};'.format(self._dbName))
            f.flush()
            out = commons.run_command(cmd, f.name)
            self.logger.info("Drop CSS database: %s",
                             self._dbName)


    def workerInsertXrootdExportPath(self):
        sql = ("SELECT * FROM qservw_worker.Dbs WHERE db='{0}';"
               .format(self._dbName))
        rows = self._sqlInterface['sock'].execute(sql)

        if len(rows) == 0:
            sql = ("INSERT INTO qservw_worker.Dbs VALUES('{0}');"
                   .format(self._dbName))
            self._sqlInterface['sock'].execute(sql)
        elif len(rows) > 1:
            self.logger.fatal("Duplicated value '%s' in qservw_worker.Dbs",
                              self._dbName)
            sys.exit(1)
