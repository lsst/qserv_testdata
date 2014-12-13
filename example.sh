set -e
set -x

QSERV_RUN_DIR="$HOME"/qserv-run/git

testdata02="$QSERV_TESTDATA_DIR"/datasets/case02/data

table=Object

qserv-data-loader.py -r --keep-chunks --keep-input-data \
--verbose-all -vvv \
-f $testdata02/common.cfg \
-f $testdata02/$table.cfg \
-d "$QSERV_RUN_DIR"/tmp/loader_chunks/$table \
-S "$QSERV_RUN_DIR"/var/lib/mysql/mysql.sock -u root -p changeme \
LSST $table $testdata02/$table.sql $testdata02/$table.txt.gz

