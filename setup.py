from setuptools import setup, find_packages

setup(
    name='rxbp',
    version='3.0.0a3',
    packages=find_packages(
        exclude=[]),
    install_requires=['rx==3.0.0b1'],
    description='A rxpy extension with back-pressure',
    author='Michael Schneeberger',
    author_email='michael.schneeb@outlook.com',
    download_url='https://github.com/MichaelSchneeberger/rxbackpressure',
)
