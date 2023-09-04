# Example 1

In this example, we process a set of CSV files to convert the temperature column from Fahrenheit to Celsius.
The resulting dataframe with updated temperatures is then saved to a new CSV file.

## Data Description

The Global Surface Summary of the Day (GSOD) dataset is derived from the Integrated Surface Hourly (ISH) dataset.
In this example, we include some of the files available for the year 2023.



## Usage instructions

1. Create a directory for caching, in case a restart is required.

    ```bash
    mkdir cache_temp
    ```

2. Set the `CACHE_DIR` environment variable
    ```
    export CACHE_DIR=cache_temp
    ```

3. For the CLI version, add the path where `example_1_tasks.py` is located to `PYTHONPATH`, e.g.:
    ```
    export PYTHONPATH=$PYTHONPATH:`pwd`
    ```

4. Run dpypeline


- Serial version

    ```
    python example_1_serial.py
    ```

- Parallel version

    ```
    python example_1_parallel.py
    ```

- Serial version (CLI)

    ```
    dpypeline -i example_1_serial.yaml > output 2> errors
    ```

- Parallel version (CLI)

    ```
    dpypeline -i example_2_serial.yaml > output 2> errors
    ```


# References

1. [GSOD Metadata](https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00516)
2. [GSOD Data for 2023](https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/2023/)
