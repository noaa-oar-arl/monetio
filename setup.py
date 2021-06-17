try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

setup(
    name='monetio',
    version='0.1',
    url='https://github.com/noaa-oar-arl/monetio',
    license='MIT',
    include_package_data=True,
    author='Barry D. Baker',
    author_email='barry.baker@noaa.gov',
    maintainer='Barry Baker',
    maintainer_email='barry.baker@noaa.gov',
    packages=find_packages(),
    package_data={'': ['data/*.txt', 'data/*.dat', 'data/*.hdf', 'data/*.ncf', 'data/*.jpg', 'data/*.png']},
    keywords=['model', 'verification', 'hysplit', 'cmaq', 'atmosphere', 'camx', 'evaluation'],
    description='The Model and Observation Evaluation Toolkit (MONET)',
    install_requires=['pandas', 'netcdf4', 'xarray', 'scipy', 'dask', 's3fs'],
)
