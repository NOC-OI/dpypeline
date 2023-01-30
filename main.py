import logging
from akita.akita import Akita
from etl.etl_pipeline import ETLPipeline
from etl.extract import setup_extract_from_server

if __name__ == "__main__":
    logging.basicConfig(
        format='%(levelname)s | %(asctime)s | %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    # Set up the actions
    setup_extract_from_server()

    akita = Akita(path="/home/joaomorado/Desktop/workspace/test_dir")
    etl = ETLPipeline(watchdog=akita)
    etl.run()

