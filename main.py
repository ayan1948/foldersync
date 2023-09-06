from typing import BinaryIO, List, Tuple
from time import sleep
from os import scandir, remove
from os.path import join, exists
import argparse
import hashlib
import logging
import sys
import shutil


def get_hash(file: BinaryIO) -> str:
    """
    Returns the hash of the file.
    :param file: File object

    :return: hexadecimal hash of the file.
    """
    hash_creator = hashlib.md5()

    # Prevents file buffer from overfilling.
    for byte_block in iter(lambda: file.read(4096), b""):
        hash_creator.update(byte_block)

    return hash_creator.hexdigest()


def initialize_logger(location) -> logging.Logger:
    """
    Initializes the logger with a file handler and returns it.

    :return: instance of logger.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    file_handle = logging.FileHandler(location)
    file_handle.setLevel(logging.INFO)
    file_handle.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handle)
    return logger


def location_check(cls):
    """
    Decorator that checks if the source and replica folders exist.
    :param cls: Class to be decorated.

    :return: object of the class FileSync.
    """

    def inner(*args):
        if exists(args[0]) and exists(args[1]):
            return cls(*args)
        else:
            raise NotADirectoryError("The locations does not exist.")

    return inner


@location_check
class FileSync:
    def __init__(self, source: str, replica: str, log: str, time: int):
        self.source = source
        self.replica = replica
        self.logger = initialize_logger(log)
        self.interval = time

    def read_folders(self) -> Tuple[List[str], List[str]]:
        """
        Reads the source and replica folders and returns the list of files in them.

        :return: Tuple of lists of files in source and replica folders.
        """
        source_files: List[str] = list(
            map(lambda entry: entry.name, filter(lambda entry: entry.is_file(), scandir(self.source)))
        )

        replica_files: List[str] = list(
            map(lambda entry: entry.name, filter(lambda entry: entry.is_file(), scandir(self.replica)))
        )

        return source_files, replica_files

    def content_sync(self, source_list: List[str], replica_list: List[str]) -> None:
        """
        Synchronizes the content of the source and replica folders.
        :param source_list: list of files in the source folder.
        :param replica_list: list of files in the replica folder.

        :return: None
        """
        for file in source_list:
            if file not in replica_list:
                shutil.copy2(join(self.source, file), self.replica)
                self.logger.info(f"File {file} copied to {self.replica}")
            else:
                source_file = join(self.source, file)
                replica_file = join(self.replica, file)
                if get_hash(open(source_file, "rb")) != get_hash(open(replica_file, "rb")):
                    shutil.copy2(source_file, replica_file)
                    self.logger.info(f"File {file} copied to {self.replica}")
                else:
                    self.logger.info(f"File {file} already exists in {self.replica}")

        for file in replica_list:
            if file not in source_list:
                remove(join(self.replica, file))
                self.logger.info(f"File {file} removed from {self.replica}")

    def sync(self) -> None:
        """
        Synchronizes the source and replica folders.

        :return: None
        """
        try:
            while True:
                source_files, replica_files = self.read_folders()
                self.content_sync(source_files, replica_files)
                sleep(self.interval)
        except KeyboardInterrupt:
            self.logger.info("Stopping file sync...")
            sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="Syncs two folders. Usage: python main.py source replica")
    parser.add_argument("source", help="Source folder")
    parser.add_argument("replica", help="Replica folder")
    parser.add_argument("-t", "--time", help="sync time is [s]", type=int, default=1)
    parser.add_argument("-l", "--log", help="log file", type=str, default="file_sync.log")
    args = parser.parse_args()
    sync = FileSync(args.source, args.replica, args.log, args.time)
    sync.sync()


if __name__ == "__main__":
    main()
