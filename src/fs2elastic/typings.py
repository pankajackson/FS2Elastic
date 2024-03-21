import os, pwd
from typing import Annotated
from pydantic import BaseModel, FilePath, DirectoryPath, HttpUrl, AfterValidator


HttpUrlString = Annotated[HttpUrl, AfterValidator(str)]
fs2elastic_home = os.path.join(pwd.getpwuid(os.getuid()).pw_dir, ".fs2elastic")


class AppConfig(BaseModel):
    app_home: DirectoryPath = fs2elastic_home
    app_config_file_path: FilePath = os.path.join(fs2elastic_home, "fs2elastic.conf")


class DatasetConfig(BaseModel):
    dataset_source_dir: DirectoryPath = pwd.getpwuid(os.getuid()).pw_dir
    dataset_supported_file_extensions: list[str] = ["csv", "xlsx", "xls", "json"]
    dataset_max_workers: int = 1
    dataset_threads_per_worker: int = 10
    dataset_chunk_size: int = 200


class ESConfig(BaseModel):
    es_hosts: list[HttpUrlString] = ["http://localhost:9200"]
    es_username: str = "elastic"
    es_password: str = ""
    es_timeout: int = 300
    es_index_prefix: str = "fs2elastic-"
    es_ssl_ca: FilePath | None = None
    es_verify_certs: bool = False


class LogConfig(BaseModel):
    log_file_path: FilePath = os.path.join(fs2elastic_home, "fs2elastic.log")
    log_max_size: int = 10 * 1024 * 1024  # 10MB
    log_backup_count: int = 5


class Config(AppConfig, DatasetConfig, ESConfig, LogConfig):
    class Config:
        extra = "forbid"
