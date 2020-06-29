#! /usr/bin/env python
from setuptools import setup, find_packages

NAME = "Oasys-Canvas-Core"
VERSION = "1.0.3"
DESCRIPTION = "Core component of Oasys Canvas"
LONG_DESCRIPTION = open("README.txt", "rt").read()

URL = "http://orange.biolab.si/"
AUTHOR = "Bioinformatics Laboratory, FRI UL"
AUTHOR_EMAIL = 'lucarebuffi@elettra.eu'

LICENSE = "GPLv3"
DOWNLOAD_URL = 'https://github.org/lucarebuffi/orange-canvas'
PACKAGES = find_packages()

PACKAGE_DATA = {
    "orangecanvas": ["icons/*.svg", "icons/*png"],
    "orangecanvas.styles": ["*.qss", "orange/*.svg"],
}

INSTALL_REQUIRES = ("six",)


CLASSIFIERS = (
    "Development Status :: 1 - Planning",
    "Environment :: X11 Applications :: Qt",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2",
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    "Operating System :: OS Independent",
    "Topic :: Scientific/Engineering :: Visualization",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Intended Audience :: Education",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Developers",
)

EXTRAS_REQUIRE = {
    # ?? :python_version<="3.2" does not work (Invalid environment
    # marker error).
    ':python_version=="2.7"': ["future", "futures", "contextlib2"],
    ':python_version=="3.0"': ["future", "futures", "contextlib2"],
    ':python_version=="3.1"': ["future", "futures", "contextlib2"],
    ':python_version=="3.2"': ["future", "futures", "contextlib2"],
    ':python_version=="3.3"': ["contextlib2"]
}

if __name__ == "__main__":
    setup(name=NAME,
          version=VERSION,
          description=DESCRIPTION,
          long_description=LONG_DESCRIPTION,
          url=URL,
          author=AUTHOR,
          author_email=AUTHOR_EMAIL,
          license=LICENSE,
          packages=PACKAGES,
          package_data=PACKAGE_DATA,
          install_requires=INSTALL_REQUIRES,
          extras_require=EXTRAS_REQUIRE,)
