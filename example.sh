set -e
set -x

QSERV_RUN_DIR="$HOME"/qserv-run/git

INPUT_DIR="$QSERV_TESTDATA_DIR"/datasets/case02/data

table=Object

qserv-data-loader.py -r --keep-chunks --keep-input-data \
--verbose-all -vvv \
-f $INPUT_DIR/common.cfg \
-f $INPUT_DIR/$table.cfg \
-d "$QSERV_RUN_DIR"/tmp/loader_chunks/$table \
-S "$QSERV_RUN_DIR"/var/lib/mysql/mysql.sock -u root -p changeme \
LSST $table $INPUT_DIR/$table.sql $INPUT_DIR/$table.txt.gz

