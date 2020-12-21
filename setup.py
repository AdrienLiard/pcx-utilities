import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent
print(find_packages(exclude=("verbatim_processor.test.*","verbatim_processor.test")))
# The text of the README file
README = (HERE / "README.md").read_text()
# This call to setup() does all the work
setup(
    name="pcx-utilities",
    version="0.1.0",
    description="A package to easily create pipeline for bva-cx-insights.com",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://bva-cx-insights.com",
    author="Adrien Liard",
    author_email="adrien.liard@bva-group.com",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
   packages=find_packages(exclude=("verbatim_processor.test.*","verbatim_processor.test")),
    include_package_data=True,
    install_requires=["pandas", "requests", "spacy", "xlrd", "openpyxl"]
)