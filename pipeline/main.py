import ingest_data
import logging
import sys


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
    )


def main():
    logging.info("Starting pipeline...")
    ingest_data.run()
    logging.info("Pipeline completed.")

if __name__ == "__main__":
    main()
