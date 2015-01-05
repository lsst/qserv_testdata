#!/bin/sh

# LSST Data Management System
# Copyright 2014 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.


# Rebuild, configure and run integration tests against a Qserv git
# repository.

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -q          quick: only rebuilds and install new Qserv code
                       instead of rebuilding from scratch,
                       and performs test case #01 instead of all
                       integration tests.

    This command will build, install and configure a Qserv mono-node
    instance using a given Qserv source repository. It will then launch
    integration tests against it. The whole process is logged to standard
    ouput and the command returns 0 if successful.

  Pre-requisite:
    source loadLSST.bash
    setup qserv_distrib -t qserv
    setup -k -r \${QSERV_SRC_DIR}

  Can be used with 'git bisect' :
    cd \${QSERV_SRC_DIR}
    git bisect start
    git bisect bad
    git bisect good previous-git-commit-id-which-pass-tests
    git bisect run `basename $0`


EOD
}

QUICK=''

# get the options
while getopts hq c ; do
    case $c in
            h) usage ; exit 0 ;;
            q) QUICK="TRUE" ;;
            \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -gt 0 ] ; then
    usage
    exit 2
fi

QSERV_RUN_DIR=$HOME/qserv-run/git

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

if [ ! "${QUICK}" ]; then
    rm -rf build lib proxy bin cfg
fi

killall mysqld mysql-proxy xrootd java python ||
{
    printf "WARN: Unable to kill some Qserv services\n"
}

# "scons install" doesn't use all proc
eupspkg -e PREFIX="${QSERV_DIR}" install
qserv-configure.py --all --force -R "${QSERV_RUN_DIR}"

# record Qserv version
GIT_HASH=$(git log -n 1 --pretty=format:"%H")
printf "%s\n" "${GIT_HASH}" > "${QSERV_RUN_DIR}"/GIT-HASH

# integration tests
"${QSERV_RUN_DIR}"/bin/qserv-start.sh

if [ "${QUICK}" ]; then
    qserv-check-integration.py --case=01 --load
else
    qserv-test-integration.py
fi
"${QSERV_RUN_DIR}"/bin/qserv-stop.sh

(
ln -sf "${QSERV_RUN_DIR}"/bin/qserv-stop.sh &&
ln -sf "${QSERV_RUN_DIR}"/bin/qserv-start.sh &&
ln -sf "${QSERV_RUN_DIR}"/bin/qserv-status.sh
) ||
printf "WARN: Unable to symlink Qserv startup scripts/n"
