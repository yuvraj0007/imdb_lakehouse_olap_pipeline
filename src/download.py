import gzip
import logging
import os
import shutil
import sys
from pathlib import Path

from src.config import DATA_RAW_PATH, DATASET_SLUG, REQUIRED_FILES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def check_kaggle_credentials() -> bool:
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        logger.info("Using Kaggle credentials from environment variables")
        return True

    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if kaggle_json.exists():
        logger.info(f"Using Kaggle credentials from {kaggle_json}")
        return True

    logger.error(
        "Kaggle API credentials not found!\n"
        "Please either:\n"
        "  1. Place kaggle.json in ~/.kaggle/kaggle.json\n"
        "  2. Set KAGGLE_USERNAME and KAGGLE_KEY environment variables"
    )
    return False


def download_from_kaggle(output_dir: str) -> None:
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        logger.error("Kaggle package not installed. Install with: pip install kaggle")
        sys.exit(1)

    logger.info(f"Downloading dataset: {DATASET_SLUG}")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET_SLUG, path=output_dir, unzip=True)
    logger.info("Download complete!")


def decompress_gz_files(output_dir: str) -> None:
    output_path = Path(output_dir)

    for gz_file in output_path.glob("*.gz"):
        output_file = output_path / gz_file.stem
        logger.info(f"Decompressing: {gz_file.name} -> {output_file.name}")

        with gzip.open(gz_file, "rb") as f_in:
            with open(output_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        gz_file.unlink()


def verify_download(output_dir: str) -> bool:
    output_path = Path(output_dir)
    missing = []

    for filename in REQUIRED_FILES:
        filepath = output_path / filename
        filepath_gz = output_path / f"{filename}.gz"

        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
            logger.info(f"  {filename} ({size_mb:.1f} MB)")
        elif filepath_gz.exists():
            size_mb = filepath_gz.stat().st_size / (1024 * 1024)
            logger.info(f"  {filename}.gz ({size_mb:.1f} MB, compressed)")
        else:
            logger.warning(f"  {filename} - MISSING")
            missing.append(filename)

    if missing:
        logger.error(f"Missing {len(missing)} required file(s): {missing}")
        return False

    total_size = sum(f.stat().st_size for f in output_path.iterdir() if f.is_file())
    logger.info(f"Total dataset size: {total_size / (1024 * 1024):.1f} MB")
    return True


def main() -> None:
    logger.info("IMDb Dataset Downloader")

    output_path = Path(DATA_RAW_PATH)
    output_path.mkdir(parents=True, exist_ok=True)

    existing_files = [f for f in REQUIRED_FILES if (output_path / f).exists() or (output_path / f"{f}.gz").exists()]

    if len(existing_files) == len(REQUIRED_FILES):
        logger.info("All required files already exist. Skipping download.")
        verify_download(DATA_RAW_PATH)
        return

    if not check_kaggle_credentials():
        sys.exit(1)

    try:
        download_from_kaggle(DATA_RAW_PATH)
        decompress_gz_files(DATA_RAW_PATH)

        if verify_download(DATA_RAW_PATH):
            logger.info("Dataset ready for ETL pipeline! Next: make etl")
        else:
            logger.error("Download verification failed!")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Download failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
