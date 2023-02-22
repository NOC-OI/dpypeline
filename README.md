# Object Store Project

## Architecture
![Pipeline architecture](/images/architecture_diagram.png "Pipeline architecture")

## How to use

### 1. Start RabbitMQ

```
```

### 2. Start a celery worker

Start a celery worker:

```bash
python -m celery -A main worker --loglevel=INFO-n ETLPipeline@%h
```

Alternatively, run the customisable `start_celery_worker.sh` script:

```bash
./start_celery_worker.sh
```

### 3. Run the ETL data pipeline

Start a celery worke:

```bash
python -u main.py
```

### 4. Monitor a Celery cluster with Flower

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
