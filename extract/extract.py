import os
import sys
from zipfile import ZipFile
import traceback
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging


def extract_zip(zip_path, extracted_data, logger):
    logger.info(f"Checking zip path: {zip_path}")
    if not os.path.exists(zip_path):
        msg = f"{zip_path} does not exist!"
        logger.error(msg)
        raise FileNotFoundError(msg)

    logger.info(f"Extracting {zip_path} to {extracted_data} ...")
    os.makedirs(extracted_data, exist_ok=True)

    try:
        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_data)
        logger.info("Extraction complete")

        os.remove(zip_path)
        logger.info(f"Removed zip file: {zip_path}")
    except Exception as e:
        logger.warning(f"Failed to extract {zip_path}: {e}")
        raise


def list_csv_files(folder_path, logger):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if csv_files:
        logger.info(f"Found {len(csv_files)} CSV file(s) in {folder_path}: {csv_files}")
    else:
        logger.warning(f"No CSV files found in {folder_path}")
    return csv_files


if __name__ == "__main__":
    try:
        logger = setup_logging("extract_csv.log")
        logger.info("Main execution started")

        if len(sys.argv) < 2:
            logger.error("Please provide extracted data directory path")
            sys.exit(1)

        EXTRACTED_DATA = sys.argv[1]

       # Zip path
        zip_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.expanduser("~/Downloads"), "archive.zip")

        logger.info(f"Zip file: {zip_path}")
        logger.info(f"Extracted data folder: {EXTRACTED_DATA}")

        extract_zip(zip_path, EXTRACTED_DATA, logger)
        csv_files = list_csv_files(EXTRACTED_DATA, logger)

        if not csv_files:
            logger.error("No CSV files found in extracted folder")
            sys.exit(1)

        logger.info(f"CSV file(s) ready for transform stage: {csv_files}")

    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)
