#!/usr/bin/env python3
"""
Script to collect and store RunPod serverless metrics in Prometheus format.

This script fetches metrics from RunPod's API for configured endpoints and writes
them to a Prometheus-compatible metrics file.
"""

import os
import sys
import httpx
import yaml
from typing import Dict, Any
from datetime import datetime, timezone


def load_config(script_path: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file.

    Args:
        script_path: Path to the directory containing the config file

    Returns:
        Dict containing the configuration

    Raises:
        SystemExit: If config file is not found
    """
    config_file = f'{script_path}/config.yml'

    try:
        with open(config_file, 'r') as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError:
        print(f'ERROR: Config file {config_file} not found!')
        sys.exit()


def get_api_key(endpoint: Dict[str, Any]) -> str:
    """
    Extract API key from endpoint configuration.

    Args:
        endpoint: Dictionary containing endpoint configuration

    Returns:
        API key string

    Raises:
        Exception: If no API key is configured
    """
    api_key = None

    if 'api_key' in endpoint:
        api_key = endpoint['api_key']
    else:
        raise Exception('No endpoint metrics api_key configured in config.yml')

    return api_key


def get_metrics(endpoint: Dict[str, Any]) -> httpx.Response:
    """
    Fetch metrics from RunPod API for a specific endpoint.

    Args:
        endpoint: Dictionary containing endpoint configuration

    Returns:
        HTTP response from the RunPod API
    """
    api_key = get_api_key(endpoint)
    endpoint_id = endpoint['id']
    interval = 'h'

    return httpx.get(
        f'https://api.runpod.ai/v2/{endpoint_id}/metrics/request_ts_v1?interval={interval}',
        headers={
            'Authorization': f'Bearer {api_key}'
        }
    )


def is_metrics_stale(metrics: Dict[str, any]) -> bool:
    """
    Check if the metrics timestamp is more than an hour old.

    Args:
        metrics: Dictionary containing a 'time' key with UTC timestamp

    Returns:
        bool: True if metrics are more than an hour old, False otherwise
    """
    metrics_time = datetime.strptime(metrics['time'], '%Y-%m-%d %H:%M:%S')
    metrics_time = metrics_time.replace(tzinfo=timezone.utc)

    current_time = datetime.now(timezone.utc)

    time_diff = (current_time - metrics_time).total_seconds()

    return time_diff > 3600


def write_metrics_data(endpoint_name: str, output_file: str, data: Dict[str, Any]) -> None:
    """
    Write metrics data to a Prometheus-format file.

    Args:
        endpoint_name: Name of the RunPod endpoint
        output_file: Path to the output file
        data: Dictionary containing metrics data
    """
    tmp_output_file = f'{output_file}.$$'
    endpoint_data = data.get('data', [])

    if len(endpoint_data):
        metrics = endpoint_data[-1]

        if not is_metrics_stale(metrics):
            f = open(tmp_output_file, 'a')
            f.write('runpod_serverless_dt_max{endpoint="' + endpoint_name + '"} ' + str(metrics['dt_max']) + '\n')
            f.write('runpod_serverless_dt_min{endpoint="' + endpoint_name + '"} ' + str(metrics['dt_min']) + '\n')
            f.write('runpod_serverless_dt_total{endpoint="' + endpoint_name + '"} ' + str(metrics['dt_total']) + '\n')
            f.write('runpod_serverless_dt_n95{endpoint="' + endpoint_name + '"} ' + str(metrics['dt_n95']) + '\n')
            f.write('runpod_serverless_dt_p70{endpoint="' + endpoint_name + '"} ' + str(metrics['dt_p70']) + '\n')
            f.write('runpod_serverless_dt_p90{endpoint="' + endpoint_name + '"} ' + str(metrics['dt_p90']) + '\n')
            f.write('runpod_serverless_dt_p98{endpoint="' + endpoint_name + '"} ' + str(metrics['dt_p98']) + '\n')
            f.write('runpod_serverless_et_max{endpoint="' + endpoint_name + '"} ' + str(metrics['et_max']) + '\n')
            f.write('runpod_serverless_et_min{endpoint="' + endpoint_name + '"} ' + str(metrics['et_min']) + '\n')
            f.write('runpod_serverless_et_total{endpoint="' + endpoint_name + '"} ' + str(metrics['et_total']) + '\n')
            f.write('runpod_serverless_et_n95{endpoint="' + endpoint_name + '"} ' + str(metrics['et_n95']) + '\n')
            f.write('runpod_serverless_et_p70{endpoint="' + endpoint_name + '"} ' + str(metrics['et_p70']) + '\n')
            f.write('runpod_serverless_et_p90{endpoint="' + endpoint_name + '"} ' + str(metrics['et_p90']) + '\n')
            f.write('runpod_serverless_et_p98{endpoint="' + endpoint_name + '"} ' + str(metrics['et_p98']) + '\n')
            f.write('runpod_serverless_retried{endpoint="' + endpoint_name + '"} ' + str(metrics['retried']) + '\n')
            f.write('runpod_serverless_requests{endpoint="' + endpoint_name + '"} ' + str(metrics['requests']) + '\n')
            f.write('runpod_serverless_completed_requests{endpoint="' + endpoint_name + '"} ' + str(metrics['completed_requests']) + '\n')
            f.write('runpod_serverless_failed_requests{endpoint="' + endpoint_name + '"} ' + str(metrics['failed_requests']) + '\n')
            f.close()
            os.rename(tmp_output_file, output_file)


def get_runpod_serverless_metrics(config: Dict[str, Any]) -> None:
    """
    Fetch and store metrics for all configured RunPod endpoints.

    Args:
        config: Dictionary containing complete configuration

    Raises:
        Exception: If API authentication fails or unexpected status code is received
    """
    filename = 'runpod_serverless_metrics.prom'
    output_file = os.path.join(config['textfile_path'], filename)

    for endpoint in config['endpoints']:
        endpoint_name = endpoint['name']
        r = get_metrics(endpoint)

        if r.status_code == 401:
            raise Exception(f'Authentication failed for {endpoint_name} endpoint, check your API key')
        elif r.status_code == 200:
            write_metrics_data(endpoint_name, output_file, r.json())
        else:
            raise Exception(f'Unexpected status code from /health endpoint: {r.status_code}')


if __name__ == '__main__':
    script_path = os.path.dirname(__file__)
    config = load_config(script_path)
    get_runpod_serverless_metrics(config)
