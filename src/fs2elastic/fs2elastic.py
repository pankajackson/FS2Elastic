import os
import pathlib
import hashlib
import time
import json
import fnmatch
import uuid
import logging
import datetime
from logging.handlers import RotatingFileHandler
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
import argparse
import pkg_resources
from fs2elastic.confbuilder import get_config
from fs2elastic.dataset_processor import DatasetProcessor
from fs2elastic.es_handler import get_es_connection
from fs2elastic.typings import Config


def get_version() -> str:
    """
    The function `get_version()` returns the version of the package "fs2elastic".
    :return: The `get_version` function is returning the version of the package "fs2elastic" using
    `pkg_resources.get_distribution("fs2elastic").version`.
    """
    return pkg_resources.get_distribution("fs2elastic").version


# Configure logging
def init_logger(log_file_path: str, log_max_size: int, log_backup_count: int):
    """
    The `init_logger` function sets up a logger in Python that writes log messages to a file with
    rotation based on size and count.
    
    :param log_file_path: The `log_file_path` parameter is the file path where the log messages will be
    written to. It should be a string representing the path to the log file
    :type log_file_path: str
    :param log_max_size: The `log_max_size` parameter specifies the maximum size in bytes that the log
    file can reach before it is rotated. This parameter is used in conjunction with the
    `RotatingFileHandler` to control the size of the log files. When the log file reaches this size, it
    will be rotated and
    :type log_max_size: int
    :param log_backup_count: The `log_backup_count` parameter specifies the number of backup log files
    to keep when rotating log files. When the log file reaches the specified size (`log_max_size`), it
    will be rotated, and the oldest log file will be removed if the number of backup log files exceeds
    the `log_backup
    :type log_backup_count: int
    """
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
    path: str, source_dir: str, supported_file_extensions: list[str]
) -> bool:
    """
    The function `is_file_extensions_supported` checks if a file matches any of the supported file
    extensions.

    :param path: The `path` parameter represents the file path that you want to check for supported file
    extensions
    :type path: str
    :param source_dir: Source_dir is the directory path where the files are located. It is used as a
    reference point to determine the relative path of the file being checked for supported extensions
    :type source_dir: str
    :param supported_file_extensions: Supported file extensions are the list of file extensions that
    your function will check against when determining if a file matches any of them. For example, if you
    want to check if a file has extensions like 'txt', 'csv', or 'pdf', you would provide those
    extensions in the `supported_file_extensions
    :type supported_file_extensions: list[str]
    :return: a boolean value - True if the file matches any of the supported extensions, and False if it
    does not match any of the supported extensions.
    """
    """Check if the file matches any supported extensions."""
    path = pathlib.PurePosixPath(path).relative_to(source_dir)
    for extension in supported_file_extensions:
        if fnmatch.fnmatch(path, f"*.{extension}"):
            return True
    return False


def process_event(config: Config, event: FileSystemEvent) -> bool:
    """
    The function `process_event` processes a file system event by syncing data to Elasticsearch and
    logging the process.

    :param config: The `config` parameter in the `process_event` function is of type `Config`. It is
    used to pass configuration settings or options to the function for processing the event. The
    specific structure and content of the `Config` class would depend on how it is defined in your
    codebase. It likely
    :type config: Config
    :param event: The `event` parameter in the `process_event` function is of type `FileSystemEvent`. It
    seems like this function processes a file system event, likely related to syncing data from a source
    file to Elasticsearch. The function generates a unique event ID using `uuid.uuid4().hex`, creates a
    `
    :type event: FileSystemEvent
    :return: The function `process_event` is returning a boolean value - `True` if the event processing
    is successful and `False` if an exception occurs during processing.
    """
    try:
        event_id = uuid.uuid4().hex
        ds_processor = DatasetProcessor(
            source_file=event.src_path, config=config, event_id=event_id
        )
        logging.info(f"SYNC_STARTED: {event_id} {event.src_path}.")
        start_time = datetime.datetime.now()
        ds_processor.es_sync()
        end_time = datetime.datetime.now()
        total_time = end_time - start_time
        logging.info(
            f"SYNC_FINISHED: {event_id} [duration: {total_time}] {event.src_path}."
        )
        return True
    except Exception as e:
        logging.error(f"SYNC_FAILED: {event_id} {event.src_path}.")
        logging.error(f"An unexpected error occurred: {e}")
        return False


def get_or_update_file_cache(config: Config, event: FileSystemEvent | None = None):
    """
    The function `get_or_update_file_cache` creates or updates a file cache stored in a JSON file based
    on a given event.

    :param config: The `config` parameter is an object that contains configuration settings for the
    application. It likely includes information such as the application's home directory and other
    settings needed for the application to run
    :type config: Config
    :param event: The `event` parameter in the `get_or_update_file_cache` function is of type
    `FileSystemEvent | None`. This means it can either be a `FileSystemEvent` object or `None`. The
    function uses this parameter to update the file cache with the hash of the file referenced by the
    event
    :type event: FileSystemEvent | None
    :return: The function `get_or_update_file_cache` returns the file cache dictionary after updating it
    with the hash of the file if an event is provided.
    """
    file_cache_path = os.path.join(config.app_home, "file_cache.json")
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


# The `FSHandler` class is a subclass of `FileSystemEventHandler` that processes file system events
# for supported file extensions and updates a file cache accordingly.
class FSHandler(FileSystemEventHandler):

    def __init__(self, config: Config):
        """
        The function initializes an object with a configuration and file cache.

        :param config: The `config` parameter is an instance of the `Config` class. It is being passed to
        the `__init__` method of a class as an argument. The `Config` class likely contains configuration
        settings or parameters that are needed for the functionality of the class
        :type config: Config
        """
        self.file_cache = get_or_update_file_cache(config=config)
        self.config = config
        super().__init__()

    def process_event(self, event: FileSystemEvent) -> None:
        """
        The `process_event` function processes a FileSystemEvent by checking if it is a directory, verifying
        if the file extension is supported, calculating the file hash, and updating the file cache based on
        the event.

        :param event: The `event` parameter in the `process_event` method is of type `FileSystemEvent`,
        which likely represents an event related to changes in the file system, such as file creation,
        modification, or deletion
        :type event: FileSystemEvent
        :return: The `process_event` method returns `None`.
        """
        if event.is_directory:
            return
        if is_file_extensions_supported(
            path=event.src_path,
            source_dir=self.config.dataset_source_dir,
            supported_file_extensions=self.config.dataset_supported_file_extensions,
        ):
            file_hash = hashlib.md5(open(event.src_path, "rb").read()).hexdigest()
            if self.file_cache.get(event.src_path) == file_hash:
                logging.info(f"Skipping event for {event.src_path}")
                return
            if process_event(config=self.config, event=event):
                self.file_cache = get_or_update_file_cache(
                    config=self.config, event=event
                )

    def on_created(self, event: FileSystemEvent) -> None:
        """
        This function is a method that is triggered when a file creation event occurs,
        and it processes the event.

        :param event: The `event` parameter in the `on_created` method is of type `FileSystemEvent`, which
        is used to represent an event related to changes in the file system, such as file creation,
        modification, or deletion
        :type event: FileSystemEvent
        """
        self.process_event(event=event)

    def on_modified(self, event: FileSystemEvent) -> None:
        """
        The `on_modified` function processes a file system event when a file is modified.

        :param event: The `event` parameter in the `on_modified` method is of type `FileSystemEvent`. It is
        used to represent an event that occurs in the file system, such as a file being modified
        :type event: FileSystemEvent
        """
        self.process_event(event=event)


def start_sync(config: Config) -> None:
    """
    The function `start_sync` sets up a file system event handler to monitor a directory for changes and
    runs indefinitely until interrupted by a keyboard interrupt.

    :param config: The `config` parameter is an object of type `Config`, which is likely a custom class
    or data structure containing configuration settings for the synchronization process. It may include
    attributes such as `dataset_source_dir` which is the path to the directory that needs to be
    monitored for changes
    :type config: Config
    """
    event_handler = FSHandler(config)
    observer = Observer()
    observer.schedule(event_handler, path=config.dataset_source_dir, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def stop_sync():
    """
    The function `stop_sync` creates an observer object, stops it, and then waits for it to finish.
    """
    observer = Observer()
    observer.stop()
    observer.join()


def main():
    """
    The `main` function in this Python script parses command line arguments, retrieves configuration
    settings, initializes logging, establishes connection to Elasticsearch, and starts the
    synchronization process.
    """
    parser = argparse.ArgumentParser(
        prog="fs2elastic",
        epilog="Please report bugs at pankajackson@live.co.uk",
        description="Sync local directory datasets to Elasticsearch in daemon mode.",
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
                log_file_path=config.log_file_path,
                log_max_size=config.log_max_size,
                log_backup_count=config.log_backup_count,
            )

            logging.info(get_es_connection(config).info())
            start_sync(config)
        except Exception as e:
            logging.error(f"Error connecting to the remote host: {e}")
            raise Exception(f"Error connecting to the remote host: {e}")


if __name__ == "__main__":
    main()
