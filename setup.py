from setuptools import setup
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()
    
setup(
    name='qasino_client',
    version='1.6.2',
    
    description='A command line client for the qasino project.',
    long_description=long_description,
    
    url='https://github.com/MediaMath/qasino',
    license='Apache 2.0',
    
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: System :: Monitoring',
        'Programming Language :: Python :: 2.7',
    ],
    
    keywords='qasino stats monitoring mediamath',
    
    package_dir = {'qasino_client': ''},
    packages=['qasino_client', 'qasino_client.lib'],
    py_modules=['qasino_client.bin.qasino_sqlclient',
                'qasino_client.bin.qasino_csvpublisher'],
    scripts=['bin/qasinosql',
             'bin/qasinocsvpub'],
    install_requires=['requests', 'txzmq'],
)
