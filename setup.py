from setuptools import setup, find_packages

setup(
    name='dbl-transpiler',
    version='0.4.0',
    packages=find_packages(exclude=['tests', 'tests.*']),
    tests_require=['pyspark==3.3.0'],
    package_data={
        'dbl-transpiler': ['lib/*.jar']
    }
)
