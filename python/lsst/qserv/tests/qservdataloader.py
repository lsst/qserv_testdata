# Loads partition and load data set and then configure Qserv
# used by test procedure

from lsst.qserv.admin import commons
from lsst.qserv.tests.sql import const, cmd, connection
import logging
import os
import sys


class QservDataLoader():

    def __init__(self, config,
                 data_reader,
                 db_name,
                 in_dirname,
                 out_dirname,
                 log_file_prefix='qserv-loader',
                 logging_level=logging.DEBUG):

        self.config = config
        self.dataReader = data_reader
        self.dataConfig = data_reader.dataConfig
        self._dbName = db_name

        self._in_dirname = in_dirname
        self._out_dirname = out_dirname

        self.logger = logging.getLogger()
        self.sock_connection_params = {
            'config': self.config,
            'mode': const.MYSQL_SOCK
            }

        self._sqlInterface = dict()

    def createAndLoadTable(self, table):
        if table in self.dataConfig['partitioned-tables']:
            self.logger.info("Create, load partitioned table %s",
                             table)
            self._createLoadPartTable(table)
        elif table in self.dataConfig['sql-views']:
            self.logger.info("Creating schema for table %s as a view",
                             table)
            self._sqlInterface['cmd'].executeFromFile(schema_filename)
        else:
            self.logger.info("Create, load non-partitioned table %s", table)
            #self._sqlInterface['cmd'].createAndLoadTable(table, schema_filename, input_filename, self.dataConfig['delimiter'])

    def _createLoadPartTable(self, table):
        ''' Partition and load Qserv data like Source and Object
        '''

        self.logger.info("Partitioning and loading data for table  '%s'" +
                         "in Qserv mono-node database", table)

        loader_cmd = [
            'qserv-data-loader.py',
            '--verbose-all',
            '-vvv',
            '--config={0}'.format(os.path.join(self.dataConfig['input-dir'],
                                               "common.cfg")),
            '--config={0}'.format(os.path.join(self.dataConfig['input-dir'],
                                               table + ".cfg")),
            '--chunks-dir={0}'.format(os.path.join(self._out_dirname,
                                                   "loader_chunks",
                                                   table)),
            '--user={0}'.format(self.config['mysqld']['user']),
            '--password={0}'.format(self.config['mysqld']['pass']),
            '--socket={0}'.format(self.config['mysqld']['sock']),
            self._dbName,
            table,
            self.dataReader.getSchemaFile(table),
            self.dataReader.getInputDataFile(table)
        ]

        out = commons.run_command(loader_cmd)
        self.logger.info("Partitioned %s data loaded (stdout : %s)", table, out)

    def connectAndInitDatabase(self):

        self._sqlInterface['sock'] = connection.Connection(**self.sock_connection_params)

        self.logger.info("Drop and create Qserv database: %s", self._dbName)
        sql_instructions= [
            "DROP DATABASE IF EXISTS %s" % self._dbName,
            "CREATE DATABASE %s" % self._dbName,
            # TODO : "GRANT ALL ON %s.* TO '%s'@'*'" % (self._dbName, self._qservUser, self._qservHost)
            "GRANT ALL ON %s.* TO 'qsmaster'@'localhost'" % (self._dbName),
            "USE %s" %  self._dbName
            ]

        for sql in sql_instructions:
            self._sqlInterface['sock'].execute(sql)

        cmd_connection_params =   self.sock_connection_params
        cmd_connection_params['database'] = self._dbName
        self._sqlInterface['cmd'] = cmd.Cmd(**cmd_connection_params)

    def workerInsertXrootdExportPath(self):
        sql = "SELECT * FROM qservw_worker.Dbs WHERE db='{0}';".format(self._dbName)
        rows = self._sqlInterface['sock'].execute(sql)

        if len(rows) == 0:
            sql = "INSERT INTO qservw_worker.Dbs VALUES('{0}');".format(self._dbName)
            self._sqlInterface['sock'].execute(sql)
        elif len(rows) > 1:
            self.logger.fatal("Duplicated value '%s' in qservw_worker.Dbs", self._dbName)
            sys.exit(1)
