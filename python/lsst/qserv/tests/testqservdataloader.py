from lsst.qserv.admin import commons, logger
from lsst.qserv.tests.qservdataloader import QservDataLoader
from lsst.qserv.tests.datareader import DataReader
import os
import sys
import unittest

class TestQservDataLoader(unittest.TestCase):

    def setUp(self):
        self.config = commons.read_user_config()
        self.logger = logger.init_default_logger(
            "TestQservDataLoader",
            log_path=self.config['qserv']['log_dir']
            )


    def test_alterTable(self):
        case_id_list = ["01","02","03"]

        for case_id in case_id_list:

            base_dir = os.getenv("QSERV_TESTDATA_DIR")

            if base_dir is None:
                self.logger.fatal("QSERV_TESTDATA_DIR environment missing.")
                sys.exit(1)

            qserv_tests_dirname = os.path.join(
                base_dir,
                'datasets',
                "case%s" % case_id
            )
            input_dirname = os.path.join(qserv_tests_dirname,'data')

            dataReader = DataReader(input_dirname, "case%s" % case_id)
            dataReader.readInputData()

            testDbName = "TestQservDataLoader%s" % case_id
            out_dir = os.path.join(self.config['qserv']['tmp_dir'],testDbName)
            qservDataLoader = QservDataLoader(
                self.config,
                dataReader.dataConfig,
                testDbName,
                out_dir,
                "TestQservDataLoader"
                )
            qservDataLoader.connectAndInitDatabase()
            for table_name in dataReader.dataConfig['partitioned-tables']:
                (schema_filename, data_filename, zipped_data_filename) =  dataReader.getSchemaAndDataFilenames(table_name)
                qservDataLoader._sqlInterface['cmd'].executeFromFile(schema_filename)
                qservDataLoader.alterTable(table_name)

def suite():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestQservDataLoader)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
