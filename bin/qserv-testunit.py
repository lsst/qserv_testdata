#!/usr/bin/env python

from  lsst.qserv.tests.testqservdataloader import TestQservDataLoader, suite
import sys
import unittest

if __name__ == '__main__':
    result  = unittest.TextTestRunner(verbosity=2).run(suite())
    retcode = int(not result.wasSuccessful())
    sys.exit(retcode)
