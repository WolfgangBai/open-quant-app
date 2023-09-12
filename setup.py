from setuptools import setup, find_packages

setup(
    name='open-quant-app',
    version='0.1.3.2',
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'matplotlib',
        'dash',
        'loguru',
        'toml'
    ],
    author='openhe',
    author_email='625529334@qq.com',
    description='A quantitative trading framework.',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    license='MIT',
    keywords='quantitative trading finance',
    url='https://github.com/openhe-hub/open-quant-app'
)
