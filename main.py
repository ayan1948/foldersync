import argparse
import hashlib
import logging
import sys
import shutil
from pathlib import Path
from typing import TypeVar, Callable
from time import sleep

T = TypeVar("T", bound=object)


def get_hash(file_path: Path) -> str:
    """
    Returns the MD5 hash of the file.
    :param file_path: Path to the file.

    :return: hexadecimal hash of the file.
    """
    hash_creator = hashlib.md5()

    with file_path.open("rb") as f:
        # Prevents file buffer from overfilling.
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_creator.update(byte_block)

    return hash_creator.hexdigest()


def initialize_logger(location: str) -> logging.Logger:
    """
    Initializes the logger with a file handler and returns it.

    :return: instance of logger.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    log_path = Path(location)
    if log_path.suffix != ".log":
        log_path = log_path.with_suffix(".log")

    # Create parent directory if it doesn't exist
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handle = logging.FileHandler(log_path)
    file_handle.setLevel(logging.INFO)
    file_handle.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handle)
    return logger


def location_check(cls) -> Callable[..., T]:
    """
    Decorator that checks if the source and replica folders exist and are directories.
    :param cls: Class to be decorated.

    :return: object of the class FileSync.
    """

    def inner(*args, **kwargs):
        # args[0] is the class 'cls' if used as a class decorator, 
        # but when called, these are the arguments passed to the constructor.
        # In main(): FileSync(args.source, args.replica, ...)
        source_path = Path(args[0])
        replica_path = Path(args[1])
        if not source_path.is_dir():
            raise NotADirectoryError(f"Source directory does not exist or is not a directory: {source_path}")
        if not replica_path.is_dir():
            raise NotADirectoryError(f"Replica directory does not exist or is not a directory: {replica_path}")
        return cls(*args, **kwargs)

    return inner


@location_check
class FileSync:
    def __init__(self, source: str, replica: str, log: str, time: int) -> None:
        """
        Initializes the FileSync class.
        :param source: Source folder.
        :param replica: Replica folder.
        :param log: Log file.
        :param time: Interval between syncs.
        """
        self.source = Path(source)
        self.replica = Path(replica)
        self.logger = initialize_logger(log)
        self.interval = time

    def print_and_log(self, message: str) -> None:
        print(message)
        self.logger.info(message)

    def run_sync(self) -> None:
        """
        Runs the sync operation for the whole directory.
        :return: None
        """
        source_files = set(
            map(lambda p: p.relative_to(self.source),
                filter(lambda p: p.is_file(), self.source.rglob("*")))
        )
        replica_files = set(
            map(lambda p: p.relative_to(self.replica),
                filter(lambda p: p.is_file(), self.replica.rglob("*")))
        )

        # Sync files
        for file_rel in source_files:
            source_file = self.source / file_rel
            replica_file = self.replica / file_rel

            if file_rel not in replica_files:
                self.print_and_log(f"Copying new file: {file_rel}")
                replica_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_file, replica_file)
            elif get_hash(source_file) != get_hash(replica_file):
                self.print_and_log(f"Updating file: {file_rel}")
                shutil.copy2(source_file, replica_file)

        for file_rel in replica_files - source_files:
            self.print_and_log(f"Removing file: {file_rel}")
            (self.replica / file_rel).unlink()

        # Sync directories
        source_dirs = set(
            map(lambda p: p.relative_to(self.source),
                filter(lambda p: p.is_dir(), self.source.rglob("*")))
        )
        replica_dirs = set(
            map(lambda p: p.relative_to(self.replica),
                filter(lambda p: p.is_dir(), self.replica.rglob("*")))
        )

        # Create missing directories
        for dir_rel in source_dirs - replica_dirs:
            self.print_and_log(f"Creating directory: {dir_rel}")
            (self.replica / dir_rel).mkdir(parents=True, exist_ok=True)

        # Remove deleted directories (in reverse order to handle nested empty dirs)
        for dir_rel in sorted(replica_dirs - source_dirs, reverse=True):
            self.print_and_log(f"Removing directory: {dir_rel}")
            shutil.rmtree(self.replica / dir_rel)

    def sync(self) -> None:
        """
        Synchronizes the source and replica folders continuously.

        :return: None
        """
        counter = 0
        try:
            while True:
                try:
                    self.print_and_log("--- Starting sync cycle ---")
                    self.run_sync()
                    self.print_and_log("--- Sync cycle finished ---")
                except PermissionError:
                    self.print_and_log("Permission denied. Retrying...")
                    counter += 1
                    if counter >= 3:
                        self.print_and_log("Permission denied multiple times. Exiting.")
                        break
                except Exception as e:
                    self.print_and_log(f"An error occurred: {e}")

                sleep(self.interval)
        except KeyboardInterrupt:
            self.print_and_log("Stopping file sync...")
            sys.exit(0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Syncs two folders. Usage: python main.py source replica")
    parser.add_argument("source", help="Source folder")
    parser.add_argument("replica", help="Replica folder")
    parser.add_argument("-t", "--time", help="sync time in seconds", type=int, default=1)
    parser.add_argument("-l", "--log", help="log file path", type=str, default="file_sync.log")
    args = parser.parse_args()

    try:
        sync = FileSync(args.source, args.replica, args.log, args.time)
        sync.sync()
    except NotADirectoryError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
