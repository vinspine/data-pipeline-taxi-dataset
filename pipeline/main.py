import ingest_data
import logging
import sys


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
    )

logger = logging.getLogger(__name__)

def main():
    logger.info("Starting the data ingestion pipeline.")
    ingest_data.run()
    logger.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
