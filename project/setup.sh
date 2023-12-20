#!/bin/bash
# change to project dir
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

python3 -m pip install -r requirements.txt
