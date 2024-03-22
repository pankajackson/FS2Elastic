import os, pwd
from typing import Annotated
from pydantic import (
    BaseModel,
    FilePath,
    DirectoryPath,
    HttpUrl,
    TypeAdapter,
    BeforeValidator,
)


# The line `HttpUrlString = Annotated[str, BeforeValidator(lambda value:
# str(TypeAdapter(HttpUrl).validate_python(value)))]` is defining a type alias `HttpUrlString` in
# Python. This type alias is specifying that `HttpUrlString` is a string that should be validated as
# an HTTP URL using the `HttpUrl` type from the `pydantic` library.
HttpUrlString = Annotated[
    str, BeforeValidator(lambda value: str(TypeAdapter(HttpUrl).validate_python(value)))
]


# define variable `fs2elastic_home` that represents the home directory path for a application
fs2elastic_home = os.path.join(pwd.getpwuid(os.getuid()).pw_dir, ".fs2elastic")


# The AppConfig class defines attributes for the home directory and configuration file path of an
# application.
class AppConfig(BaseModel):
    app_home: DirectoryPath = fs2elastic_home
    app_config_file_path: FilePath = os.path.join(fs2elastic_home, "fs2elastic.conf")


# This Python class `DatasetConfig` defines configuration parameters for dataset processing.
class DatasetConfig(BaseModel):
    dataset_source_dir: DirectoryPath = pwd.getpwuid(os.getuid()).pw_dir
    dataset_supported_file_extensions: list[str] = ["csv", "xlsx", "xls", "json"]
    dataset_max_workers: int = 1
    dataset_threads_per_worker: int = 10
    dataset_chunk_size: int = 200


# This Python class defines configuration settings for connecting to an Elasticsearch cluster.
class ESConfig(BaseModel):
    es_hosts: list[HttpUrlString] = ["http://localhost:9200"]
    es_username: str = "elastic"
    es_password: str = ""
    es_timeout: int = 300
    es_index_prefix: str = "fs2elastic-"
    es_ssl_ca: FilePath | None = None
    es_verify_certs: bool = False


# The class `LogConfig` defines attributes for configuring logging settings such as log file path,
# maximum size, and backup count.
class LogConfig(BaseModel):
    log_file_path: FilePath = os.path.join(fs2elastic_home, "fs2elastic.log")
    log_max_size: int = 10 * 1024 * 1024  # 10MB
    log_backup_count: int = 5


# The class `Config` inherits from `AppConfig`, `DatasetConfig`, `ESConfig`, and `LogConfig` to create a consolidated config class, with the
# additional configuration setting `extra = "forbid"`.
class Config(AppConfig, DatasetConfig, ESConfig, LogConfig):
    class Config:
        extra = "forbid"
