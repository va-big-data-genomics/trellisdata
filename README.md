# trellisdata
Python package with classes and methods used in implementing a Trellis data management system.

# To install new commits locally
$ python3 -m pip install ./

# To upload package to PyPi
$ vi setup.cfg # bump the version number
$ python3 -m build
$ python3 -m twine upload dist/*
