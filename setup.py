import setuptools
from altonomy.apl_bots import __version__

setuptools.setup(
    name='altonomy_apl_bots',
    packages=['altonomy.apl_bots'],
    description='Altonomy APL Bots',
    long_description=open('README.md').read(),
    version=__version__,
    url='https://wiki.altono.me',
    author='Wee Howe Ang',
    author_email='weehowe.ang@altonomy.com',
    python_requires='>=3.6.0',
    entry_points={'console_scripts': ['altaplbot = altonomy.apl_bots.service:main']},
    keywords=['legacy bot'],
    install_requires=[
        'altonomy-client==1.0.3',
        'altonomy-services>=2.0.8',
        'zerorpc',
        'pandas',
        'numpy',
        'sqlalchemy==1.4.41',
        'requests',
        'mysql-connector==2.2.9',
        'altonomy-loggers>=0.1.2',
        'altonomy-models==0.6.22',
        'altonomy-exchanges==1.0.150',
        'cachetools',
        'altonomy-ref-data-api==0.0.24',
        'nested-lookup',
        'altonomy-price-server-api>=0.0.5',
        'pyzmq==22.3.0'
    ],
)
