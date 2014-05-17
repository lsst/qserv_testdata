
# load the data in LSST db and launch the tests related to case01
qserv-benchmark.py --case=01 --load

---

# launch the tests related to case01
# LSST db has to already contain test data
qserv-benchmark.py --case=01
