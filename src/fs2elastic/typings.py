from typing import Annotated
from pydantic import BaseModel, FilePath, DirectoryPath, HttpUrl, AfterValidator
from pathlib import Path

HttpUrlString = Annotated[HttpUrl, AfterValidator(str)]


class AppConfig(BaseModel):
    app_home: DirectoryPath
    app_config_file_path: FilePath


class SourceConfig(BaseModel):
    source_dir: DirectoryPath
    source_supported_file_extensions: list[str]


class ESConfig(BaseModel):
    es_hosts: list[HttpUrlString]
    es_username: str
    es_password: str
    es_index_prefix: str
    es_ssl_ca: FilePath | None
    es_verify_certs: bool
    es_max_dataset_chunk_size: int


class LogConfig(BaseModel):
    log_file_path: FilePath
    log_max_size: int
    log_backup_count: int


class Config(AppConfig, SourceConfig, ESConfig, LogConfig):
    class Config:
        extra = "forbid"
