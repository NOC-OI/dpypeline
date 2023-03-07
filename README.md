# dpypeline
![Continuous Integration](https://github.com/NOC-OI/object-store-project/actions/workflows/main.yml/badge.svg)
![Test Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/JMorado/c20a3ec5262f14d970a462403316a547/raw/pytest_coverage_report_main.json)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Program for creating data pipelines triggered by file creation events.

# Version

0.1.0-beta.1

# Pipeline architecture

## Celery-based pipeline
![Celery-based pipeline architecture](/images/celery_pipeline.png "Celery-based pipeline architecture")

## Thread-based pipeline
![Thread-based pipeline architecture](/images/thread_pipeline.png "Thread-based pipeline architecture")

# Python enviroment setup

Setup the environment using conda (or mamba):

```
conda create --name <environment_name> --file requirements.txt -c conda-forge python=3.10
conda activate <environment_name>
```

Alternatively, use `virtualenv` and `pip` to setup the environment:

```bash
python -m venv <environment_name>
source <envionment_name>/bin/activate
pip install -r requirements.txt
```

# How to use

## 1. Install the dpypeline package

Install dpypeline using pip:

```bash
pip install -i https://test.pypi.org/simple/ dpypeline
```


## 2. Start RabbitMQ locally (Optional)

Set up a local instance of RabbitMQ using Docker:

``` bash
docker pull rabbitmq:3-management
docker run --rm -it -p 15672:15672 -p 5672:5672  rabbitmq:3-management
```

The rabbitMQ management interface can be access on the url http://localhost:15672


NOTE: If RabbitMQ is run locally, set the BROKER_URL env variable to amqp://guest:guest@localhost. The default username and password are uest.


## 3. Start a celery worker

Start a celery worker:

```bash
python -m celery -A main worker --loglevel=INFO -n ETLPipeline@%h
```

Alternatively, run the customisable `start_celery_worker.sh` script:

```bash
./start_celery_worker.sh
```

## 4. Run the ETL data pipeline

Run one of the examples in the examples directory, e.g.:

```bash
python -u examples/example_celery.py
```

## 4. Monitor a Celery cluster with Flower (Optional)

Install Flower using pip:

```bash
pip install flower
```

Launch the Flower server at specified port (default is 5555, so `--port=5555` can be ommited):

```
python -m celery -A main flower --port=5555
```

Alternatively, run Flower via docker:

```
docker run -p 5555:5555 mher/flower
```

Access Flower on the url http://localhost:5555/

## Unit tests

Run tests using `pytest` in the main directory:

```
pip install pytest
pytest
```

## Environment variables

There are a few env variables that need to be set so that the application can run correctly:

- `BROKER_URL`: URL of the rabbitMQ broker to connect to.
- `CACHE_DIR`: Path to the cache directory.



## Filesystems

### Credentials to access the object store (.json file)

From inside JASMIN:

    {
        "token": <Token generated using the Caringo Portal>,
        "secret": <Secret generated using the Caringo Portal>,
        "endpoint_url": "https://noc-msm-o.s3.jc.rl.ac.uk"
    }

External access, from outside JASMIN:

    {
        "token": <Token generated using the Caringo portal>,
        "secret": <Secret generated using the Caringo portal>,
        "endpoint_url": "https://noc-msm-o.s3-ext.jc.rl.ac.uk"
    }
