from elasticsearch import Elasticsearch, helpers
from fs2elastic.typings import Config
from retry import retry


def get_es_connection(config: Config) -> Elasticsearch:

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
    es_client = get_es_connection(config)
    helpers.bulk(client=es_client, actions=actions)
