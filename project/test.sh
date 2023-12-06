#!/bin/bash
# change to project dir
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

# run the tests
python3 ./test.py
