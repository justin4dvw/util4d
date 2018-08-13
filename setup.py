from setuptools import setup

setup(name='util4d',
      version='0.1',
      description='Utility for 4d data environment',
      url='http://github.com/storborg/funniest',
      author='Justin Jeong',
      author_email='justin@4dvw.com',

      packages=['util4d'],
      install_requires=[
          'pymongo',
          'pyaml',
          'geocoder'
      ],
      zip_safe=False
      )
