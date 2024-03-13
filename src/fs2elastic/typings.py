from typing import List, Optional

AppConfig = {
    "app_home": str,
    "app_config_file_path": str,
}
SourceConfig = {
    "source_dir": str,
    "source_supported_file_extensions": List[str],
}
ESConfig = {
    "es_hosts": List[str],
    "es_username": str,
    "es_password": str,
    "es_ssl_ca": str,
    "es_verify_certs": bool,
}
LogConfig = {
    "log_file_path": str,
    "log_max_size": int,
    "log_backup_count": int,
}

FullConfig = {
    **Optional(AppConfig),
    **SourceConfig,
    **ESConfig,
    **LogConfig,
}
