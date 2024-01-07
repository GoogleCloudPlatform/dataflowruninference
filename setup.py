from setuptools import setup, find_packages
import setuptools
setup(name='runinferencellm',
  version='0.1',
  py_modules=['runinferenceutil'],
  description='RunInferenceLLM',
  install_requires=[
    'torch==2.0.1',
    'transformers==4.36.0',
    'google-cloud-translate==3.12.0'
  ],
  packages = setuptools.find_packages()
 )
