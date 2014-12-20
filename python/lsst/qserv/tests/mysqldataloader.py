from  lsst.qserv.admin import commons
from  lsst.qserv.tests.sql import const, cmd, connection, schema
import logging
import os
import re
import shutil

class MysqlDataLoader():

    def __init__(self, config, data_reader, db_name, out_dirname,
                 log_file_prefix='qserv-loader', logging_level=logging.DEBUG):
        self.config = config
        self.dataReader = data_reader
        self.dataConfig = data_reader.dataConfig
        self._dbName = db_name

        self._out_dirname = out_dirname

        self.logger = logging.getLogger()
        self.sock_connection_params = {
            'config' : self.config,
            'mode' : const.MYSQL_SOCK
            }

        self._sqlInterface = dict()

    def createLoadTable(self, table):

        #if table_name in self.dataConfig['sql-views']:
        #    self.logger.info("Creating schema for table %s as a view" % table_name)
        #    self._sqlInterface['cmd'].executeFromFile(schema_filename)
        #else:
        #    self.logger.info("Creating and loading non-partitioned table %s" % table_name)

        self._callLoader(table)

    def _callLoader(self, table):
        '''
        Call Qserv loader to load plain-MySQL table
         '''

        self.logger.info("Create, load table %s", table)

        tmp_dir = self.config['qserv']['tmp_dir']
        run_dir = self.config['qserv']['run_base_dir']
        loader_cmd = [
            'qserv-data-loader.py',
            '--verbose-all',
            '-vvv',
            '--config={0}'.format(os.path.join(self.dataConfig['input-dir'],
                                               "common.cfg")),
            '--no-css',
            '--user={0}'.format(self.config['mysqld']['user']),
            '--password={0}'.format(self.config['mysqld']['pass']),
            '--socket={0}'.format(self.config['mysqld']['sock']),
            '--delete-tables',
            '--skip-partition',
            '--one-table',
            self._dbName,
            table,
            self.dataReader.getSchemaFile(table)]

        data = self.dataReader.getInputDataFile(table)
        if data is not None:
            loader_cmd.append(self.dataReader.getInputDataFile(table))

        out = commons.run_command(loader_cmd)
        self.logger.info("Partitioned %s data loaded (stdout : %s)", table, out)


    def prepareDatabase(self):
        """
        Connect to MySQL via sock
        Create MySQL database
        Create MySQL command-line client
        """

        self._sqlInterface['sock'] = connection.Connection(**self.sock_connection_params)

        self._sqlInterface['sock'].dropAndCreateDb(self._dbName)
        self._sqlInterface['sock'].setDb(self._dbName)

        cmd_connection_params =   self.sock_connection_params
        cmd_connection_params['database'] = self._dbName
        self._sqlInterface['cmd'] = cmd.Cmd(**cmd_connection_params)

