from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [Extension('src.backtest.*', ['src/backtest/*.pyx'])]

setup(name='arbie', ext_modules=cythonize(extensions, annotate=True))