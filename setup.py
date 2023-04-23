from setuptools import find_packages, setup

NAME = "cnvxfeast"
REQUIRES_PYTHON = "==3.9.0"

setup(
    name=NAME,
    version="0.0.1",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    packages=find_packages(include=["cnvxfeaststore"]),
    install_requires=["feast==0.30.2"],
)
