#!/bin/bash
# Pre-requisite:
#   source loadLSST.bash
#   setup qserv_distrib -t qserv
#   setup -k -r ${QSERV_SRC_DIR}
set -x
set -e

QSERV_RUN_DIR=$HOME/qserv-run/test-branch

# load eups "setup" function
if [ -r "${EUPS_DIR}" -a -x "${EUPS_DIR}" ]; then
    . "${EUPS_DIR}/bin/setups.sh"
else
    printf "ERROR: ${EUPS_DIR} must be a readable directory"
    exit 1
fi

if [ ! -r "${QSERV_DIR}" -o ! -r "${QSERV_DIR}/SConstruct" ]; then
    printf "ERROR: ${QSERV_DIR} must be a source directory"
    exit 1
fi

# cleaning, if source directory
cd ${QSERV_DIR}
rm -rf build lib proxy bin cfg
killall mysqld mysql-proxy xrootd java python ||
{
    printf "Unable to kill some services"
}

# "scons install" doesn't use all proc
eupspkg -e PREFIX=${QSERV_DIR} install
qserv-configure.py --all --force -R ${QSERV_RUN_DIR}
${QSERV_RUN_DIR}/bin/qserv-start.sh
qserv-test-integration.py
#qserv-check-integration.py --case=01 --load
${QSERV_RUN_DIR}/bin/qserv-stop.sh
