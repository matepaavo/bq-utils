import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name="bq-utils",
    version="0.1.0",
    description="A collection of BigQuery utilities",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/matepaavo/bq-utils",
    author="Balázs Máté",
    author_email="matepaavo@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
    ],
    packages=["bqutils"],
    include_package_data=True,
    install_requires=["google-cloud-bigquery"],
    entry_points={
        "console_scripts": [
            "bqutils=bqutils.__main__:main",
        ]
    },
)
