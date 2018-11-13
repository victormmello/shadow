from setuptools import setup

setup(name='shadow_packages',
      version='0.1',
      description='Internal Packages',
      url='http://github.com/storborg/shadow_packages',
      author='Victor Meireles',
      author_email='victor@marciamello.com.br',
      license='MIT',
      packages=['shadow_database','shadow_helpers'],
      zip_safe=False)