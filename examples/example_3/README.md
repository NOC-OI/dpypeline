# Example 3

## Instructions on how to send a frozen dataset to the object store in Zarr format

1.  Create a workspace for the data pipeline

    ```bash
    mkdir pipeline_workspace
    cd pipeline_workspace
    ```

2.  Create the input file necessary to generate the templates

    To do this, we must have a file that contains our dataset
    dimensions, coordinates, and variables.

    ```bash
    dpypelinetools --create-template-input --dataset /gws/nopw/j04/nemo_vol1/ORCA0083-N006/means/1964/ORCA0083-N06_1964m10T.nc
    ```

    This command will create a template.yaml file with the following
    structure:

    ```yaml
    # Input file to generate a template for dpypeline
    # Automatically created by dpypelinetools 0.1.0-beta.1
    create_template:
      files: '#TODO'
      multiple_file_template: '#TODO'
      single_file_template: template.zarr
      coords:
        - nav_lat
        - nav_lon
        - deptht
        - time_instant
        - time_counter
        - time_centered
      dims:
        - y
        - x
        - deptht
        - time_counter
      variables:
        - nav_lat
        - nav_lon
        - deptht
        - sst
        - time_instant
        - time_counter
        - sss
        - ssh
        - time_centered
        - potemp
        - salin
        - tossq
        - zossq
        - mldkz5
        - mldr10_1
        - wfo
        - rsntds
        - tohfls
        - sosflxdo
        - taum
        - sowindsp
        - soprecip
        - e3t
      concat_dim: '#TODO'
      chunks: auto
      sort_by_concat_dim: true
      eager_coords: true
      region_dict: true
      fill_value: null
      open_mfdataset_kwargs:
        combine: by_coords
        compat: override
        combine_attrs: override
        data_vars: all
        coords: minimal
        join: outer
        parallel: true
    ```

    -   Feel free to change the coordinates (coords) and variables that
        you want your template to have.\

    -   Be careful when modifying the dimensions (dims), as coordinates
        and variables depend on them.\

    -   Note that there are some fields marked as \'\#TODO\' that
        require user intervention. In this example, we will set those
        to:\
        - files:
        \"/gws/nopw/j04/nemo\_vol1/ORCA0083-N006/means/\*\*/ORCA0083-N06\_\*m\*T.nc

        Glob expression used to find the files of our dataset.

        -   multiple\_file\_template: see \"3. Add the object store
            configuration for remote storage\"

            Store or path to directory in local or remote file system.
            If we want to store our multifile dataset template in the
            local file system, pass a path. Otherwise, pass a mapper.

        -   concat\_dim: time\_counter

            Dimension(s) to concatenate files along.

    -   More information about the kwargs passed to the create\_template
        function can be found here
        [here](https://github.com/NOC-OI/dpypelinetools/blob/master/src/dpypelinetools/template.py).

    -   More information about the kwargs passed to the open\_mfdataset
        function can be found
        [here](https://docs.xarray.dev/en/stable/generated/xarray.open_mfdataset.html).

3.  Add the object store configuration for remote storage

    In this example, we will store our multifile dataset on the Jasmin
    object store.\
    To do this, we must send the multifile dataset template to the
    object store.\
    This can be easily achieved by adding some configs at the top of the
    template.yaml file, so that it reads:

    ```yaml
    # Destination servers configuration
    servers:
      &object_store !ObjectStoreS3
      anon: False
      store_credentials_json: credentials.json

    # Single-run functions
    single_run_functions:
      - !Method
        instance: *object_store
        method: create_bucket
        bucket: joaomorado

      - &bucket_mapper !Method
        instance: *object_store
        method: get_mapper
        bucket: joaomorado/n06.zarr

    create_template:
      files: "/gws/nopw/j04/nemo_vol1/ORCA0083-N006/means/**/ORCA0083-N06_*m*T.nc"
      multiple_file_template: *bucket_mapper
      single_file_template: template.zarr
      coords:
        - nav_lat
        - nav_lon
        - deptht
        - time_instant
        - time_counter
        - time_centered
      dims:
        - y
        - x
        - deptht
        - time_counter
      variables:
        - nav_lat
        - nav_lon
        - deptht
        - sst
        - time_instant
        - time_counter
        - sss
        - ssh
        - time_centered
        - potemp
        - salin
        - tossq
        - zossq
        - mldkz5
        - mldr10_1
        - wfo
        - rsntds
        - tohfls
        - sosflxdo
        - taum
        - sowindsp
        - soprecip
        - e3t
      concat_dim: time_counter
      chunks:
        time_counter: 1
        deptht: 5
        x: 577
        y: 577
      sort_by_concat_dim: true
      eager_coords: true
      region_dict: true
      fill_value: null
      open_mfdataset_kwargs:
        engine: h5netcdf
        combine: by_coords
        compat: override
        combine_attrs: override
        data_vars: all
        coords: minimal
        join: outer
        parallel: true
    ```

    Note that here we also explicitly defined the chunking scheme we
    want for our dataset.

4.  Create the templates

    To create the templates, simply run:

    ```bash
    dpypelinetools --create-template -i template.yaml 2> errors
    ```

    This command generates three outputs:

    -   A local template.zarr that will be used by dpypeline (single
        file template).
    -   A remote template (metadata + coordinates data, since
        eager\_coords=True) located at the object store to which we will
        write to specific regions (multiple file template).
    -   A file named template\_region\_dict.yaml (since
        regions\_dict=True), which will be used by dpypeline. This file
        contains the regions to where each file will be written in the
        object store.

    **NOTE!!!** If during this process you get erros such as:

        ValueError: coordinate 'time_instant' not present in all datasets.

    this indicates that the listed coordinates must be removed from the
    \"coords\" list in the template.yaml file.\
    Since these coordinates are not present in all datasets, they must
    be \"downgraded\" from coordinates to variables.\
    As variables, they will appear as empty arrays (arrays filled with
    NaNs) when not present.

5.  Run dpypeline

    The dataset can be finally sent to the object store by running
    dpypeline:

    ```bash
    #!/bin/bash

    micromamba activate dpypeline

    # Dask settings
    export DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES=50
    export DASK_DISTRIBUTED__DEPLOY__LOST_WORKER_TIMEOUT=60
    export DASK_DISTRIBUTED__WORKER__LIFETIME__RESTART=True

    # Dpypeline settings
    export CACHE_DIR=$PWD/TMP

    dpypeline -i pipeline.yaml > output 2> errors
    ```

    where pipeline.yaml is the input file for dpypeline. It is in this
    file that all jobs and tasks are defined, as well as the dpypeline
    configurations. It has the following adaptable structure:

    ```yaml
    # Akita configuration
    akita:
      &akita !Akita
      path: /gws/nopw/j04/nemo_vol1/ORCA0083-N006/means
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
      - &template !Function
        function: xarray.open_zarr
        store: template.zarr

      - &reference_names !Function
        function: dpypelinetools.tasks.pipeline_tasks.create_reference_names_dict
        dataset: *template

      - &regions !Function
        function: dpypelinetools.utils.load_yaml
        yaml_file: template_region_dict.yaml

      - &bucket_mapper !Method
        instance: *jasmin_os
        method: get_mapper
        bucket: joaomorado/n06.zarr

    # Pipeline jobs configuration
    jobs:
      &job_os !Job
        name: "send-to-object-store"
        tasks:
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.open_dataset
       persist: false
       chunks:
         time_counter: 1
         deptht: 5
         x: 577
         y: 577
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.rename_vars
       reference_names: *reference_names
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.clean_dataset
       fill_value: null
       missing_value: 1e20
       threshold: 1e-6
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.match_to_template
       template: *template
          - !Task
       function: dpypelinetools.tasks.pipeline_tasks.to_zarr
       store: *bucket_mapper
       mode: "r+"
       region_dict: *regions

    # Pipeline configuration
    pipeline:
      &pipeline !BasicPipeline
      jobs:
        - *job_os

    # Dask client/cluster configuration
    dask_client:
      &cluster_client !DaskClient
      cluster: dask_jobqueue.SLURMCluster
      name: dask-cluster
      scale:
        jobs: 8
      processes: 16
      cores: 16
      queue: par-single
      memory: "128 GB"
      walltime: "48:00:00"
      job_script_prologue:
        - micromamba activate dpypeline

    # Event consumer configuration
    event_consumer:
      !ConsumerParallel
      akita: *akita
      job_producer: *pipeline
      cluster_client: *cluster_client
      workers_per_event: 2
    ```

    In this example, only functions from the module
    `dpypelinetools.tasks.pipeline_tasks` were used in the tasks. However, any Python function can be virtually used as a task in the
    pipeline.

    Note, though, that you might have to include in `PYTHONPATH`` any
    modules that do not belong to `dpypeline` or `dpypelinetools` and that
    contain functions you would like to use as tasks.

## Credentials to access the object store (.json file)

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
