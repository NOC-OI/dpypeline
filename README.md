# dpypeline
![Continuous Integration](https://github.com/NOC-OI/object-store-project/actions/workflows/main.yml/badge.svg)
[![PyPI version](https://badge.fury.io/py/dpypeline.svg)](https://badge.fury.io/py/dpypeline)
![Test Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/JMorado/c20a3ec5262f14d970a462403316a547/raw/pytest_coverage_report_main.json)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Program for creating data pipelines triggered by file creation events.

## Version

0.1.0-beta.4

## Python enviroment setup

To utilise this package, it should be installed within a dedicated Conda environment. You can create this environment using the following command:

```
conda create --name <environment_name> python=3.10
```

To activate the conda environment use:
```
conda activate <environment_name>
```

Alternatively, use `virtualenv` to setup and activate the environment:

```
python -m venv <environment_name>
source <envionment_name>/bin/activate
```

## Installation

1. Clone the repository:

```
git clone git@github.com:NOC-OI/dpyepline.git
```

2. Navigate to the package directory:

After cloning the repository, navigate to the root directory of the package.

3. Install in editable mode:

To install `dpypeline` in editable mode, execute the following comman from the root directory:

```
pip install -e .
```

This command will install the library in editable mode, allowing you to make changes to the code if needed.

4. Alternative installation methods:

- Install from the GitHub repository directly:


```
pip install git+https://github.com/NOC-OI/dpypeline.git@main#egg=dpypeline
```

- Install from the PyPI repository:

```
pip install dpypeline
```

## Unit tests

Run tests using `pytest` in the main directory:

```
pip install pytest
pytest
```
## Examples

### Python scripts

Examples of Python scripts explaining how to use this package can be found in the examples directory.

### Command line interface (CLI)

The CLI provided by this package allows you to execute data pipelines defined in YAML files; however, it offers less flexibility compared to using the Python scripts. To run the dpypeline CLI, type, e.g., the following command:

```bash
dpypeline -i <input_file> > output 2> errors
```

#### Flags description


- `-h` or `--help`: show an help message
- `-i INPUT_FILE` or `--input INPUT_FILE`: Filepath to the pipeline YAML file (by default `pipelien.yaml`)
- `-v` or `--version`: show dpypeline's version umber


### Environment variables

There are a few environment variables that need to be set so that the application can run correctly:

- `CACHE_DIR`: Path to the cache directory.

## Software Workflow Overview

## Pipeline architectures

![Dpypeline diagram](/images/dpypeline_diagram.png)


### Thread-based pipeline

In the thread-based pipeline, `Akita` enqueues events into an in-memory queue. These events are subsequently consumed by `ConsumerSerial`, which generates jobs for sequential execution within the `ThreadPipeline` (an alias for `BasicPipeline`).

### Parallel pipeline

In the parallel pipeline, `Akita` enqueues events into an in-memory queue. These events are then consumed by `ConsumerParallel`, which generates futures that are executed concurrently by multiple Dask workers.
