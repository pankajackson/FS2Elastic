import os
import pwd
import toml
from pathlib import Path
from fs2elastic.typings import Config

fs2elastic_home = os.path.join(pwd.getpwuid(os.getuid()).pw_dir, ".fs2elastic")
defaults = {
    "AppConfig": {
        "app_home": fs2elastic_home,
        "app_config_file_path": os.path.join(fs2elastic_home, "fs2elastic.conf"),
    },
    "SourceConfig": {
        "source_dir": pwd.getpwuid(os.getuid()).pw_dir,
        "source_supported_file_extensions": ["csv"],
    },
    "ESConfig": {
        "es_hosts": ["http://localhost:9200"],
        "es_username": "elastic",
        "es_password": "",
        "es_index_prefix": "fs2elastic-",
        "es_ssl_ca": None,
        "es_verify_certs": False,
        "es_max_dataset_chunk_size": 100,
    },
    "LogConfig": {
        "log_file_path": str(os.path.join(fs2elastic_home, "fs2elastic.log")),
        "log_max_size": 10 * 1024 * 1024,  # 10MB
        "log_backup_count": 5,
    },
}


def conf_initializer(config_file_path: str) -> str:
    if not os.path.exists(config_file_path):
        file = open(config_file_path, "w")
        config = {
            "AppConfig": {**defaults["AppConfig"]},
            "SourceConfig": {**defaults["SourceConfig"]},
            "ESConfig": {**defaults["ESConfig"]},
            "LogConfig": {**defaults["LogConfig"]},
        }
        toml.dump(config, file)
        file.close()
        print(f"Please configure config file {config_file_path}")
    return config_file_path


def get_value_of(key, config_file_path):
    # Read configuration from  ~/.fs2elastic/fs2elastic.conf
    with open(config_file_path, "r") as f:
        toml_config = toml.load(f)
    if key.startswith("app_"):
        try:
            return toml_config["AppConfig"][key]
        except KeyError:
            return defaults["AppConfig"][key]
    elif key.startswith("source_"):
        try:
            return toml_config["SourceConfig"][key]
        except KeyError:
            return defaults["SourceConfig"][key]
    elif key.startswith("es_"):
        try:
            return toml_config["ESConfig"][key]
        except KeyError:
            return defaults["ESConfig"][key]
    elif key.startswith("log_"):
        try:
            return toml_config["LogConfig"][key]
        except KeyError:
            return defaults["LogConfig"][key]
    else:
        raise ValueError(f"Unknown Key {key}")


def toml_conf_reader(config_file_path: str) -> Config:
    config = {
        "app_home": Path(get_value_of("app_home", config_file_path)),
        "app_config_file_path": get_value_of("app_config_file_path", config_file_path),
        "source_dir": Path(get_value_of("source_dir", config_file_path)),
        "source_supported_file_extensions": get_value_of(
            "source_supported_file_extensions", config_file_path
        ),
        "es_hosts": get_value_of("es_hosts", config_file_path),
        "es_username": get_value_of("es_username", config_file_path),
        "es_password": get_value_of("es_password", config_file_path),
        "es_index_prefix": get_value_of("es_index_prefix", config_file_path),
        "es_ssl_ca": get_value_of("es_ssl_ca", config_file_path),
        "es_verify_certs": get_value_of("es_verify_certs", config_file_path),
        "es_max_dataset_chunk_size": get_value_of(
            "es_max_dataset_chunk_size", config_file_path
        ),
        "log_file_path": get_value_of("log_file_path", config_file_path),
        "log_max_size": int(
            get_value_of("log_max_size", config_file_path),
        ),
        "log_backup_count": int(
            get_value_of("log_backup_count", config_file_path),
        ),
    }
    return Config(**config)


def get_config(
    config_file_path: str = defaults["AppConfig"]["app_config_file_path"],
) -> Config:
    if not os.path.exists(defaults["AppConfig"]["app_home"]):
        os.makedirs(defaults["AppConfig"]["app_home"])
    return toml_conf_reader(conf_initializer(config_file_path))
