# dpypeline
![Continuous Integration](https://github.com/NOC-OI/object-store-project/actions/workflows/main.yml/badge.svg)
[![PyPI version](https://badge.fury.io/py/dpypeline.svg)](https://badge.fury.io/py/dpypeline)
![Test Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/JMorado/c20a3ec5262f14d970a462403316a547/raw/pytest_coverage_report_main.json)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Program for creating data pipelines triggered by file creation events.

# Version

0.1.0-beta.1

# Python enviroment setup

Setup the environment using conda (or mamba):

```bash
conda create --name <environment_name> python=3.10
conda activate <environment_name>
```

Alternatively, use `virtualenv` to setup the environment:

```bash
python -m venv <environment_name>
source <envionment_name>/bin/activate
```

# Installation

Install the `dpypeline` package using pip:

```bash
pip install dpypeline
```

# Unit tests

Run tests using `pytest` in the main directory:

```
pip install pytest
pytest
```

# Environment variables

There are a few environment variables that need to be set so that the application can run correctly:

- `CACHE_DIR`: Path to the cache directory.
- `BROKER_URL`: URL of the rabbitMQ broker to connect to (only required when using the celery-based pipeline).


# Pipeline architectures

## Thread-based pipeline
![Thread-based pipeline architecture](/images/thread_pipeline.png "Thread-based pipeline architecture")

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
