#!/bin/sh

python -m pytest
echo Flake8:
flake8 --max-line-length=120 brunodb

