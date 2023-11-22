#!/bin/bash
# change to project dir
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

# run the pipeline
python3 ./pipeline.py
