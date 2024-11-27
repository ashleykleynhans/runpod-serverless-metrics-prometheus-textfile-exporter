#!/usr/bin/env python3
import os
import sys
import httpx
import yaml


def load_config(script_path):
    try:
        config_file = f'{script_path}/config.yml'

        with open(config_file, 'r') as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError:
        print(f'ERROR: Config file {config_file} not found!')
        sys.exit()


def get_api_key(endpoint, config):
    api_key = None

    if 'api_key' in endpoint:
        api_key = endpoint['api_key']
    else:
        raise Exception('No endpoint metrics api_key configured in config.yml')

    return api_key


def get_metrics(endpoint, config):
    api_key = get_api_key(endpoint, config)
    endpoint_id = endpoint['id']
    interval = 'h'

    return httpx.get(
        f'https://api.runpod.ai/v2/{endpoint_id}/metrics/request_ts_v1?interval={interval}',
        headers={
            'Authorization': f'Bearer {api_key}'
        }
    )


def write_metrics_data(endpoint_name, tmp_output_file, endpoint, data):
    endpoint_data = data.get('data', [])

    if len(endpoint_data):
        metrics = endpoint_data[-1]
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
        f.write('runpod_serverless_time{endpoint="' + endpoint_name + '"} ' + str(metrics['time']) + '\n')
        f.close()


def get_runpod_serverless_metrics(config):
    filename = 'runpod_serverless_metrics.prom'
    output_file = os.path.join(config['textfile_path'], filename)
    tmp_output_file = f'{output_file}.$$'

    for endpoint in config['endpoints']:
        endpoint_name = endpoint['name']
        r = get_metrics(endpoint, config)

        if r.status_code == 401:
            raise Exception(f'Authentication failed for {endpoint_name} endpoint, check your API key')
        elif r.status_code == 200:
            write_metrics_data(endpoint_name, tmp_output_file, endpoint, r.json())
        else:
            raise Exception(f'Unexpected status code from /health endpoint: {r.status_code}')

    os.rename(tmp_output_file, output_file)


if __name__ == '__main__':
    script_path = os.path.dirname(__file__)
    config = load_config(script_path)
    get_runpod_serverless_metrics(config)
