# Instructions on how to send NetCDF files

1.  Create a workspace for the data pipeline and the directory where
    cache will be stored

    ```bash
    mkdir pipeline_workspace
    cd pipeline_workspace
    mkdir TMP
    ```

2.  Create the input file

    **Serial example** (pipeline\_serial.yaml)

    ```yaml
    # Akita configuration
    akita:
      &akita !Akita
      path: /gws/nopw/j04/nemo_vol1/ORCA0083-N006/means/
      patterns: "**/ORCA*-N06_*m*T.nc"
      ignore_patterns: null
      ignore_directories: true
      case_sensitive: true
      glob_kwargs: null
      monitor: false

    # Destination servers configuration
    servers:
      &jasmin_os !ObjectStoreS3
      anon: False
      store_credentials_json: credentials.json

    # Single-run functions create data required by the pipeline
    single_run_functions:
      - !Method
        instance: *jasmin_os
        method: create_bucket
        bucket: &bucket netcdf-files-repo

    # Pipeline jobs configuration
    jobs:
      - &job_os !Job
        name: "send-to-object-store"
        tasks:
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.open_dataset
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.clean_dataset
       fill_value: null
       missing_value: 1e20
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.write_netcdf_dataset_to_bucket
       s3fs_instance: *jasmin_os
       bucket: *bucket

      - &job_et !Job
        name: "send-to-elastic-tape"
        tasks:
          - !Task
       function: dpypelinetools.tasks.jdma_tasks.upload_files
       name: jmorado
       workspace: nemo
       request_type: PUT
       label: n06-dataset
       storage: elastictape
       credentials:
         username: jmorado

    # Pipeline configuration
    pipeline:
      &pipeline !BasicPipeline
      jobs:
        - *job_os
        - *job_et

    # Event consumer configuration
    event_consumer:
      !ConsumerSerial
      akita: *akita
      job_producer: *pipeline
    ```

    **Parallel example** (pipeline\_parallel.yaml)

    ```yaml
    # Akita configuration
    akita:
      &akita !Akita
      path: /gws/nopw/j04/nemo_vol1/ORCA0083-N006/means/
      patterns: "**/ORCA*-N06_*m*T.nc"
      ignore_patterns: null
      ignore_directories: true
      case_sensitive: true
      glob_kwargs: null
      monitor: false

    # Destination servers configuration
    servers:
      &jasmin_os !ObjectStoreS3
      anon: False
      store_credentials_json: credentials.json

    # Single-run functions create data required by the pipeline
    single_run_functions:
      - !Method
        instance: *jasmin_os
        method: create_bucket
        bucket: &bucket netcdf-files-repo

    # Pipeline jobs configuration
    jobs:
      - &job_os !Job
        name: "send-to-object-store"
        tasks:
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.open_dataset
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.clean_dataset
       fill_value: null
       missing_value: 1e20
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.write_netcdf_dataset_to_bucket
       s3fs_instance: *jasmin_os
       bucket: *bucket

      - &job_et !Job
        name: "send-to-elastic-tape"
        tasks:
          - !Task
       function: dpypelinetools.tasks.jdma_tasks.upload_files
       name: jmorado
       workspace: nemo
       request_type: PUT
       label: n06-dataset
       storage: elastictape
       credentials:
         username: jmorado

    # Pipeline configuration
    pipeline:
      &pipeline !BasicPipeline
      jobs:
        - *job_os
        - *job_et

    # Dask client/cluster configuration
    dask_client:
      &cluster_client !DaskClient
      cluster: dask_jobqueue.SLURMCluster
      scale:
        jobs: 1
      processes: 8
      cores: 8
      queue: test
      memory: "128 GB"
      walltime: "4:00:00"

    # Event consumer configuration
    event_consumer:
      !ConsumerParallel
      akita: *akita
      job_producer: *pipeline
      cluster_client: *cluster_client
      workers_per_event: 2
    ```

3.  Run dpypeline

    To run the serial version of the pipeline, use the following script:

    ```bash
    #!/bin/bash
    # Active the python environment
    micromamba activate dpypeline

    # Dpypeline settings
    export CACHE_DIR=$PWD/TMP

    # Run dpypeline
    dpypeline -i pipeline_serial.yaml > output 2> errors
    ```

    To run the parallel version of the pipeline, use the following
    script:

    ```bash
    #!/bin/bash

    micromamba activate dpypeline

    # Dask settings
    export DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES=50
    export DASK_DISTRIBUTED__DEPLOY__LOST_WORKER_TIMEOUT=60
    export DASK_DISTRIBUTED__WORKER__LIFETIME__RESTART=True

    # Dpypeline settings
    export CACHE_DIR=$PWD/TMP

    # Run dpypeline
    dpypeline -i pipeline_parallel.yaml > output 2> errors
    ```
