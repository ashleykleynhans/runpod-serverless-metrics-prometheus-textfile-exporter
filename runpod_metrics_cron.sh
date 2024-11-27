#!/usr/bin/env bash
path=`dirname -- "$0"`
cd ${path}
source venv/bin/activate
python3 monitor_runpod_serverless_metrics.py
