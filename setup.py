from setuptools import setup, find_packages
from os import path


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='rxbp',
    version='3.0.0a9',
    install_requires=['rx'],
    description='An RxPY extension with back-pressure',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/MichaelSchneeberger/rxbackpressure',
    author='Michael Schneeberger',
    author_email='michael.schneeb@outlook.com',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords=['rx reactive extension back-pressure backpressure flowable multicast'],
    packages=find_packages(include=['rxbp', 'rxbp.*']),
    python_requires='>=3.7',
)
