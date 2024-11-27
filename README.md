# RunPod Serverless Metrics Prometheus Textfile Exporter

## Installing

### Clone the repo

```bash
git clone https://github.com/ashleykleynhans/runpod-serverless-metrics-prometheus-textfile-exporter.git
cd runpod-serverless-metrics-prometheus-textfile-exporter
```

### Create and activate a Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### Install the dependencies

```bash
pip3 install -r requirements.txt
```

### Create a config file

```bash
cp config.yml.example config.yml
```

### Add your RunPod API key and endpoints to the config file

1. Edit config.yml
2. Ensure that `textfile_path` is pointing to the correct path for your Prometheus Text File exporter directory.
3. Replace `YOUR_RUNPOD_API_KEY` with your RunPod API key.
4. Replace each of the endpoints with your own RunPod endpoints, giving each a unique name, and using the
   endpoint ID from the RunPod Serverless web console.

### Run the script via a cron job to generate the files

Create a cron job that runs the script `runpod_metrics_cron.sh` at your preferred interval.
