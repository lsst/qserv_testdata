# all commands can be used in either mono or multi node

# to run all tests
qserv-test-integration.py

# to run integration tests for one case
qserv-check-integration.py --case=01 --load

# to run integration tests for one case, only for qserv
qserv-check-integration.py --case=01 --load --mode=qserv

# to run integration tests for one case, only for mysql
qserv-check-integration.py --case=01 --load --mode=mysql

# if you ran a given test previously, you can skip --load

# to test individual query, determine proxy port number,
# e.g., by looking at $QSERV_DIR/var/log/mysql-proxy.log
# and run
mysql --port <port> --protocol TCP <dbName> -A -e "<the query>"
