import argparse
import hashlib
import logging
import sys
import shutil
from typing import BinaryIO, List, Iterable, TypeVar, Callable
from time import sleep
from os import remove, walk, scandir
from os.path import join, exists, relpath

T = TypeVar("T", bound=object)


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

    location = location if location.endswith(".log") else location + "file_sync.log"
    file_handle = logging.FileHandler(location)

    file_handle.setLevel(logging.INFO)
    file_handle.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handle)
    return logger


def location_check(cls) -> Callable[..., T]:
    """
    Decorator that checks if the source and replica folders exist.
    :param cls: Class to be decorated.

    :return: object of the class FileSync.
    """

    def inner(*args):
        for arg in args[:2]:
            if not exists(arg):
                raise NotADirectoryError("The locations does not exist.")
        return cls(*args)

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
        self.source = source
        self.replica = replica
        self.logger = initialize_logger(log)
        self.interval = time

    def print_and_log(self, message: str) -> None:
        print(message)
        self.logger.info(message)

    def content_sync(self, source_list: List[str], replica_list: List[str]) -> None:
        """
        Synchronizes the content of the source and replica folders.
        :param source_list: list of files in the source folder.
        :param replica_list: list of files in the replica folder.

        :return: None
        """
        for file in source_list:
            if file not in replica_list:
                shutil.copy2(join(self.source, file), join(self.replica, file))
                self.print_and_log(f"File '{file}' copied to '{self.replica}'")
            else:
                source_file = join(self.source, file)
                replica_file = join(self.replica, file)
                if get_hash(open(source_file, "rb")) != get_hash(open(replica_file, "rb")):
                    shutil.copy2(source_file, replica_file)
                    self.print_and_log(f"File '{file}' copied to '{self.replica}'")

        for file in replica_list:
            if file not in source_list:
                remove(join(self.replica, file))
                self.print_and_log(f"File '{file}' removed from '{self.replica}'")

    def folder_sync(self, source_list: List[str], replica_list: List[str]) -> None:
        """
        Synchronizes the folders in the source and replica folders.
        :param source_list: list of folders in the source folder.
        :param replica_list: list of folders in the replica folder.

        :return: None
        """
        for folder in source_list:
            if folder not in replica_list:
                shutil.copytree(join(self.source, folder), join(self.replica, folder))
                self.print_and_log(f"Folder '{folder}' copied to '{self.replica}'")

        for folder in replica_list:
            if folder not in source_list:
                shutil.rmtree(join(self.replica, folder))
                self.print_and_log(f"Folder '{folder}' removed from '{self.replica}'")

    def run_sync(self) -> None:
        """
        Runs the sync operation for the whole directory.
        :return: None
        """
        for source_dir, source_sub_dirs, source_files in walk(self.source, topdown=True):
            relative_dir = relpath(source_dir, self.source)
            replica_dir = source_dir.replace(self.source, self.replica)

            if not exists(replica_dir):
                shutil.copytree(source_dir, replica_dir)
                self.print_and_log(f"Folder {relative_dir} copied to {self.replica}")

            file_iter: Iterable[object] = filter(lambda item: item.is_file(), scandir(replica_dir))
            replica_sub_files = list(map(lambda item: join(relative_dir, item.name), file_iter))
            source_files_path = list(map(lambda item: join(relative_dir, item), source_files))

            folder_iter: Iterable[object] = filter(lambda item: item.is_dir(), scandir(replica_dir))
            replica_sub_dirs = list(map(lambda item: join(relative_dir, item.name), folder_iter))
            source_dir_path = list(map(lambda item: join(relative_dir, item), source_sub_dirs))

            self.folder_sync(source_dir_path, replica_sub_dirs)
            self.content_sync(source_files_path, replica_sub_files)

    def sync(self) -> None:
        """
        Synchronizes the source and replica folders.

        :return: None
        """
        counter = 0
        try:
            while counter < 3:
                try:
                    self.run_sync()
                except PermissionError:
                    self.print_and_log("Permission denied...")
                    counter += 1
                sleep(self.interval)
        except KeyboardInterrupt:
            self.print_and_log("Stopping file sync...")
            sys.exit(0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Syncs two folders. Usage: python main.py source replica")
    parser.add_argument("source", help="Source folder")
    parser.add_argument("replica", help="Replica folder")
    parser.add_argument("-t", "--time", help="sync time is [s]", type=int, default=1)
    parser.add_argument("-l", "--log", help="log file", type=str, default="file_sync.log")
    args = parser.parse_args()
    sync = FileSync(args.source, args.replica, args.log, args.time)
    sync.sync()


# main.py "C:\Users\%User%\Downloads\Documents" "C:\Users\%User%\Downloads\doc_replica"
if __name__ == "__main__":
    main()
