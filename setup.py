from setuptools import setup, find_packages
import unittest


# def my_test_suite():
#     test_loader = unittest.TestLoader()
#     test_suite = test_loader.discover('test_rxbp', pattern='test_*.py')
#     return test_suite


setup(
    name='rxbp',
    version='3.0.0a7',
    packages=find_packages(),
    install_requires=['rx==3.0.1'],
    description='A rxpy extension with back-pressure',
    author='Michael Schneeberger',
    author_email='michael.schneeb@outlook.com',
    download_url='https://github.com/MichaelSchneeberger/rxbackpressure',
    test_suite='tests',
)
