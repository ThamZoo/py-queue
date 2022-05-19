from setuptools import setup
import os
import re

from mypyc.build import mypycify

# TAG = os.environ.get("CI_COMMIT_TAG")
# VERSION = re.findall(r"^py-queue-v(\d+\.\d+\.\d+$)", TAG)
# print(f"Building package version {VERSION}")
setup(
    name='py_queue',
    version="0.0.7",
    packages=['py_queue'],
    ext_modules=mypycify([
        '--disallow-any-unimported',
        '--disallow-any-generics',
        '--disallow-untyped-defs',
        '--disallow-incomplete-defs',
        'py_queue'
    ], debug_level="0"),
    install_requires=["orjson", "mypy"]
)
