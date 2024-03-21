import os
import toml
from pathlib import Path
from fs2elastic.typings import Config, AppConfig, DatasetConfig, ESConfig, LogConfig


# The `defaults` dictionary is being initialized with key-value pairs where the keys represent
# different configuration types ("AppConfig", "DatasetConfig", "ESConfig", "LogConfig") and the values
# are the default configurations for each type obtained by calling the `model_dump()` method on
# instances of the respective configuration classes (`AppConfig`, `DatasetConfig`, `ESConfig`,
# `LogConfig`). These default configurations will be used as fallback values when specific
# configuration keys are not found in the configuration file or environment variables.
defaults = {
    "AppConfig": AppConfig().model_dump(),
    "DatasetConfig": DatasetConfig().model_dump(),
    "ESConfig": ESConfig().model_dump(),
    "LogConfig": LogConfig().model_dump(),
}


def conf_initializer(config_file_path: str) -> str:
    """
    The function `conf_initializer` creates a configuration file with default settings if it does not
    already exist.

    :param config_file_path: The `config_file_path` parameter is a string that represents the file path
    where the configuration file will be created or updated
    :type config_file_path: str
    :return: The function `conf_initializer` is returning the `config_file_path` variable, which is a
    string representing the path to the configuration file that was either created or already existed.
    """
    if not os.path.exists(config_file_path):
        file = open(config_file_path, "w")
        config = {
            "AppConfig": {**defaults["AppConfig"]},
            "DatasetConfig": {**defaults["DatasetConfig"]},
            "ESConfig": {**defaults["ESConfig"]},
            "LogConfig": {**defaults["LogConfig"]},
        }
        toml.dump(config, file)
        file.close()
        print(f"Please configure config file {config_file_path}")
    return config_file_path


def get_value_of(key: str, config_file_path):
    """
    The function `get_value_of` reads a configuration file and retrieves values based on the provided
    key prefix.

    :param key: The `key` parameter in the `get_value_of` function is a string that is used to retrieve
    a specific value from a configuration file based on certain prefixes. The function checks the prefix
    of the key and then looks up the corresponding value in the configuration file. The prefixes it
    checks for are "
    :type key: str
    :param config_file_path: The `config_file_path` parameter in the `get_value_of` function is the path
    to the configuration file from which the function reads the configuration settings. This file is
    expected to be in TOML format and contains configurations for different sections like `AppConfig`,
    `DatasetConfig`, `ESConfig`,
    :return: The function `get_value_of` reads a configuration file specified by `config_file_path` and
    returns the value associated with the given `key`. Depending on the prefix of the key (e.g., "app_",
    "dataset_", "es_", "log_"), it looks up the corresponding section in the configuration file
    (`toml_config`) and returns the value associated with that key. If the key
    """
    # Read configuration from  ~/.fs2elastic/fs2elastic.conf
    with open(config_file_path, "r") as f:
        toml_config = toml.load(f)
    if key.startswith("app_"):
        try:
            return os.getenv(f"FS2ES_{key.upper()}", toml_config["AppConfig"][key])
        except KeyError:
            return defaults["AppConfig"][key]
    elif key.startswith("dataset_"):
        try:
            return os.getenv(f"FS2ES_{key.upper()}", toml_config["DatasetConfig"][key])
        except KeyError:
            return defaults["DatasetConfig"][key]
    elif key.startswith("es_"):
        try:
            return os.getenv(f"FS2ES_{key.upper()}", toml_config["ESConfig"][key])
        except KeyError:
            return defaults["ESConfig"][key]
    elif key.startswith("log_"):
        try:
            return os.getenv(f"FS2ES_{key.upper()}", toml_config["LogConfig"][key])
        except KeyError:
            return defaults["LogConfig"][key]
    else:
        raise ValueError(f"Unknown Key {key}")


def toml_conf_reader(config_file_path: str) -> Config:
    """
    The function `toml_conf_reader` reads configuration values from a TOML file and returns a `Config`
    object.

    :param config_file_path: The `config_file_path` parameter is the path to the configuration file from
    which you want to read the configuration values. This function reads various configuration values
    from the specified TOML configuration file and constructs a `Config` object with those values. The
    `get_value_of` function is used to retrieve the
    :type config_file_path: str
    :return: An instance of the `Config` class with various attributes initialized with values read from
    a TOML configuration file specified by the `config_file_path`.
    """

    config = Config(
        app_home=Path(get_value_of("app_home", config_file_path)),
        app_config_file_path=get_value_of("app_config_file_path", config_file_path),
        dataset_source_dir=Path(get_value_of("dataset_source_dir", config_file_path)),
        dataset_supported_file_extensions=get_value_of(
            "dataset_supported_file_extensions", config_file_path
        ),
        dataset_max_workers=get_value_of("dataset_max_workers", config_file_path),
        dataset_threads_per_worker=get_value_of(
            "dataset_threads_per_worker", config_file_path
        ),
        dataset_chunk_size=get_value_of("dataset_chunk_size", config_file_path),
        es_hosts=get_value_of("es_hosts", config_file_path),
        es_username=get_value_of("es_username", config_file_path),
        es_password=get_value_of("es_password", config_file_path),
        es_timeout=get_value_of("es_timeout", config_file_path),
        es_index_prefix=get_value_of("es_index_prefix", config_file_path),
        es_ssl_ca=get_value_of("es_ssl_ca", config_file_path),
        es_verify_certs=get_value_of("es_verify_certs", config_file_path),
        log_file_path=get_value_of("log_file_path", config_file_path),
        log_max_size=int(
            get_value_of("log_max_size", config_file_path),
        ),
        log_backup_count=int(
            get_value_of("log_backup_count", config_file_path),
        ),
    )
    return config


def get_config(
    config_file_path: str = defaults["AppConfig"]["app_config_file_path"],
) -> Config:
    """
    The function `get_config` reads a TOML configuration file and returns a Config object.

    :param config_file_path: The `config_file_path` parameter is the path to the configuration file that
    will be used to initialize the application configuration. It is set to a default value obtained from
    `defaults["AppConfig"]["app_config_file_path"]` if no value is provided when calling the
    `get_config` function
    :type config_file_path: str
    :return: The function `get_config` is returning the result of calling `toml_conf_reader` with the
    argument `conf_initializer(config_file_path)`.
    """
    if not os.path.exists(defaults["AppConfig"]["app_home"]):
        os.makedirs(defaults["AppConfig"]["app_home"])
    return toml_conf_reader(conf_initializer(config_file_path))
