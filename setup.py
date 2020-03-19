from setuptools import find_packages

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# finally, we can pass all this to distutils
setup(
    name="ImageSaverLib",
    version="1.0.0",
    author="Florian Sauer",
    description="Module to store and hide data in images or other files and upload them to Online Services",
    license="GPL-3.0",
    # keywords = "example documentation tutorial",
    url="https://github.com/FlorianSauer/ImageSaver",
    packages=find_packages(),
)
