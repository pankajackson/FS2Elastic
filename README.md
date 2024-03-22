# FS2Elastic

Python based CLI for Syncing File System based Data (CSV file etc.) to Elasticsearch.

### Features

- Real time csv file to Elasticsearch dataset syncing
- Configurable with custom config file

### Installation

```bash
pip install fs2elastic
```

### Configuration

configuration file present at `~/.fs2elastic/fs2elastic.conf`.

- please create configuration file if not present.
- replace values with correct values
- remove configs that does not require

#### default configuration:

```bash
[AppConfig]
app_home = "/home/john/.fs2elastic"
app_config_file_path = "/home/john/.fs2elastic/fs2elastic.conf"

[DatasetConfig]
dataset_source_dir = "/home/john/csv_data_set"
dataset_supported_file_extensions = [ "csv", "xlsx", "xls", "json"]
dataset_max_workers = 1
dataset_threads_per_worker = 10
dataset_chunk_size = 200

[ESConfig]
es_hosts = [ "https://localhost:9200",]
es_username = "elastic"
es_password = ""
es_timeout = 300
es_index_prefix = "fs2es-"
es_ssl_ca = ""
es_verify_certs = false

[LogConfig]
log_file_path = "/home/john/.fs2elastic/fs2elastic.log"
log_max_size = 10485760
log_backup_count = 5
```

### Usage

#### with Default config file

```bash
fs2elastic
```

#### with Custom config file

```bash
fs2elastic -c <config file path>
eg: fs2elastic -c ~/Documents/fs2elastic_custom_config.conf
```

##### Help

```bash
fs2elastic -h
```

##### Version

```bash
fs2elastic -v
```

#### Use as a system service

`NOTE: Required sudo permission`

```bash
# install Package
sudo pip install fs2elastic
```

```bash
# Create fs2elastic home directory somewhere. for eg /tmp/fs2elastic/
mkdir /tmp/fs2elastic/
```

```bash
# create config file at /tmp/fs2elastic/fs2elastic.conf inside fs2elastic home directory created above
[AppConfig]
app_home = "/home/john/.fs2elastic"
app_config_file_path = "/home/john/.fs2elastic/fs2elastic.conf"

[DatasetConfig]
dataset_source_dir = "/home/john/csv_data_set"
dataset_supported_file_extensions = [ "csv", "xlsx", "xls", "json"]
dataset_max_workers = 1
dataset_threads_per_worker = 10
dataset_chunk_size = 200

[ESConfig]
es_hosts = [ "https://localhost:9200",]
es_username = "elastic"
es_password = ""
es_timeout = 300
es_index_prefix = "fs2es-"
es_ssl_ca = ""
es_verify_certs = false

[LogConfig]
log_file_path = "/home/john/.fs2elastic/fs2elastic.log"
log_max_size = 10485760
log_backup_count = 5

# replace with correct values
```

```bash
# create service file at /etc/systemd/system/fs2elastic.service  with following content

[Unit]
Description=FS2Elastic Service
After=network.target

[Service]
User=root
Group=root
ExecStart=/usr/bin/fs2elastic -c /tmp/fs2elastic/fs2elastic.conf
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
# Reloading Change to Systemd Unit Files
sudo systemctl daemon-reload
```

```bash
# Start service
sudo systemctl start fs2elastic
```

```bash
# Enable service to start on boot
sudo systemctl enable fs2elastic
```

```bash
# check status
sudo systemctl status fs2elastic
```

```bash
# check logs
sudo tail -f /var/log/fs2elastic.log
```

```bash
# To stop service
sudo systemctl stop fs2elastic
```

```bash
# To remove service
sudo systemctl disable fs2elastic
sudo rm /etc/systemd/system/fs2elastic.service

```

### Uninstall

```bash
pip uninstall fs2elastic
```

### Who do I talk to?

- Repo owner or admin
- Other community or team contact
