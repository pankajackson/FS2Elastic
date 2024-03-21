from elasticsearch import Elasticsearch, helpers
from fs2elastic.typings import Config
from retry import retry


def get_es_connection(config: Config) -> Elasticsearch:
    """
    The function `get_es_connection` creates and returns an Elasticsearch client connection using the
    provided configuration settings.

    :param config: The `config` parameter is an object of type `Config` that contains the configuration
    settings needed to establish a connection to Elasticsearch. It likely includes the following
    attributes:
    :type config: Config
    :return: The function `get_es_connection` is returning an Elasticsearch client object that is
    connected to the Elasticsearch cluster using the provided configuration settings such as hosts,
    authentication credentials, timeout, SSL certificates, and certificate verification settings.
    """
    es_client = Elasticsearch(
        hosts=config.es_hosts,
        basic_auth=(config.es_username, config.es_password),
        request_timeout=config.es_timeout,
        ca_certs=config.es_ssl_ca,
        verify_certs=config.es_verify_certs,
    )
    return es_client


@retry(tries=10, delay=1, backoff=2, max_delay=10)
def put_es_bulk(config: Config, actions) -> None:
    """
    The `put_es_bulk` function send bulk actions to Elasticsearch with specified parameters with retry functionality.

    :param config: The `config` parameter is an object of type `Config` that is passed to the
    `put_es_bulk` function. This object likely contains configuration settings or parameters needed to
    establish a connection to an Elasticsearch (ES) server
    :type config: Config
    :param actions: The `actions` parameter in the `put_es_bulk` function is a list of actions to be
    performed in Elasticsearch. Each action in the list represents a single operation to be executed in
    Elasticsearch, such as indexing, updating, or deleting a document. These actions are typically
    formatted as dictionaries following the Elasticsearch
    """
    es_client = get_es_connection(config)
    helpers.bulk(client=es_client, actions=actions)
