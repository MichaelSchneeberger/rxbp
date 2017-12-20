from setuptools import setup, find_packages

setup(
    name='rxbackpressure',
    version='0.0.1',
    packages=find_packages(
        exclude=[]),
    install_requires=['rx==1.6.0'],
    description='An rxpy extension with backpressure',
    author='Michael Schneeberger',
    author_email='michael.schneeb@outlook.com',
)
