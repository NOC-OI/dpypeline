import logging
from akita.akita import Akita
from etl.etl_pipeline import ETLPipeline
from etl.extract import extract_from_server
from etl.load import setup_load_action, load_to_server
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

    # Set up the actions (the order of the actions matters)
    setup_load_action(load_to_server, engine="zarr", mapper=jasmin.get_mapper("etl"))

    # Setup Akita and run the ETL pipeline
    akita = Akita(path="/home/joaomorado/Desktop/workspace/test_dir")
    etl = ETLPipeline(watchdog=akita)
    etl.run()

