from setuptools import setup, find_packages

setup(
    name='dbl-transpiler',
    version='0.4.0',
    packages=find_packages(exclude=['tests', 'tests.*']),
    tests_require=['pyspark==3.3.0'],
    author='Databricks Labs',
    description='SIEM-to-PySpark Transpiler',
    url='https://github.com/databrickslabs/transpiler',
    license = 'Databricks License',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
)
