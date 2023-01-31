import logging
from akita.akita import Akita
from etl.etl_pipeline import ETLPipeline
from etl.extract import setup_extract_action
from etl.load import setup_load_action
from etl.transform import setup_transform_action
from etl.actions import LoadToServer, OpenDataSet, CleanDataSet
from filesystems.object_store import ObjectStoreS3

if __name__ == "__main__":
    logging.basicConfig(
        format='%(levelname)s | %(asctime)s | %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    # Create instance of the JASMIN object store
    jasmin = ObjectStoreS3(anon=False,
                           store_credentials_json="/home/joaomorado/PycharmProjects/"
                                                  "msm_project/jasmin_object_store_credentials.json")

    # Define tree actions that will be executed in the ETL pipeline
    open_dataset = OpenDataSet()
    clean_dataset = CleanDataSet(dependencies=[open_dataset])
    load_to_server = LoadToServer(dependencies=[clean_dataset])

    # Set up the actions of each stage of the ETL pipeline (the order of the actions matters)
    setup_extract_action(open_dataset, engine="netcdf4")
    setup_transform_action(clean_dataset)
    setup_load_action(load_to_server, load_option="zarr", mapper=jasmin.get_mapper("testmorado"))

    # Setup Akita and run the ETL pipeline
    akita = Akita(path="/home/joaomorado/Desktop/workspace/test_dir")
    etl = ETLPipeline(watchdog=akita)
    etl.run()
