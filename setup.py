from setuptools import setup, find_packages

setup(
    name='dispatcher',
    version='0.1.10',
    packages=find_packages(),
    url='https://github.com/ShagaleevAlexey/dispatcher',
    license='',
    author='Alexey Shagaleev',
    author_email='alexey.shagaleev@yandex.ru',
    description='Base application for dispatchers',
    install_requires=[
        'asynctnt==0.2.0',
        'asynctnt-queue==0.0.5',
        'structlog==18.1.0',
        'python-rapidjson==0.6.3',
        'sanic_envconfig==1.0.1'
    ]
)
