import os
import pathlib
import hashlib
import time
import json
import fnmatch
import logging
from logging.handlers import RotatingFileHandler
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import argparse
import pkg_resources
from fs2elastic.confbuilder import get_config
from fs2elastic.dataset_processor import DatasetProcessor
from fs2elastic.es_handler import get_es_connection

# from fs2elastic.fs2elastic_types import ESConfig, AppConfig, SourceConfig, LogConfig
from typing import List


def get_version():
    return pkg_resources.get_distribution("fs2elastic").version


# Configure logging
def init_logger(log_file_path, log_max_size, log_backup_count):
    logger = logging.getLogger("")
    logger.setLevel(logging.INFO)
    file_handler = RotatingFileHandler(
        log_file_path, maxBytes=log_max_size, backupCount=log_backup_count
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
    )
    # Add rotating file handler to the root logger
    logger.addHandler(file_handler)


def is_file_extensions_supported(
    path: str, source_dir: str, supported_file_extensions: List[str]
) -> bool:
    """Check if the file matches any supported extensions."""
    path = pathlib.PurePosixPath(path).relative_to(source_dir)
    for extension in supported_file_extensions:
        if fnmatch.fnmatch(path, f"*.{extension}"):
            return True
    return False


def process_event(event, config):
    # NOTE: DO SOMETHING HERE
    try:
        ds_processor = DatasetProcessor(source_file=event.src_path, config=config)
        logging.info(f"SYNC_STARTED: {event.src_path}.")
        ds_processor.es_sync(chunk_size=100)
        # NOTE: DO SOMETHING HERE
        logging.info(f"SYNC_SUCCESS: {event.src_path}.")
    except Exception as e:
        logging.error(f"SYNC_FAILED: {event.src_path}.")
        logging.error(f"An unexpected error occurred: {e}")


def get_or_update_file_cache(config, event=None):
    file_cache_path = os.path.join(config["app_home"], "file_cache.json")
    if not os.path.exists(file_cache_path):
        with open(file_cache_path, "w") as f:
            f.write("{}")
    with open(file_cache_path, "r") as f:
        file_cache = json.load(f)
    if event:
        file_hash = hashlib.md5(open(event.src_path, "rb").read()).hexdigest()
        file_cache[event.src_path] = file_hash
        with open(file_cache_path, "w") as f:
            json.dump(file_cache, f, indent=4)
    return file_cache


class FSHandler(FileSystemEventHandler):

    def __init__(self, config) -> None:
        self.file_cache = get_or_update_file_cache(config)
        self.config = config
        super().__init__()

    def on_closed(self, event):
        if event.is_directory:
            return
        if is_file_extensions_supported(
            path=event.src_path,
            source_dir=self.config["source_dir"],
            supported_file_extensions=self.config["source_supported_file_extensions"],
        ):
            file_hash = hashlib.md5(open(event.src_path, "rb").read()).hexdigest()
            if self.file_cache.get(event.src_path) == file_hash:
                logging.info(f"Skipping event for {event.src_path}")
                return
            self.file_cache = get_or_update_file_cache(self.config, event)
            process_event(event, self.config)


def start_sync(config):
    event_handler = FSHandler(config=config)
    observer = Observer()
    observer.schedule(event_handler, path=config["source_dir"], recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def stop_sync():
    # Add any cleanup steps if needed
    observer = Observer()
    observer.stop()
    observer.join()


def main():
    parser = argparse.ArgumentParser(
        prog="fs2elastic",
        epilog="Please report bugs at pankajackson@live.co.uk",
        description="Sync local directory to remote directory in daemon mode.",
    )
    parser.add_argument(
        "-c",
        "--config",
        required=False,
        type=str,
        help=f"Config file path. default: ~/.fs2elastic/fs2elastic.conf",
        metavar="<path>",
    )
    parser.add_argument(
        "-v", "--version", required=False, action="store_true", help="Show version"
    )

    args = parser.parse_args()
    if args.version:
        print(f"fs2elastic: {get_version()}")
    else:
        config = get_config(args.config) if args.config else get_config()
        try:
            init_logger(
                log_file_path=config["log_file_path"],
                log_max_size=config["log_max_size"],
                log_backup_count=["log_backup_count"],
            )

            logging.info(get_es_connection(config).info())
            start_sync(config)
        except Exception as e:
            logging.error(f"Error connecting to the remote host: {e}")
            raise Exception(f"Error connecting to the remote host: {e}")


if __name__ == "__main__":
    main()
