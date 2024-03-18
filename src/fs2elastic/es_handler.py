import logging
from elasticsearch import Elasticsearch, helpers
from fs2elastic.typings import Config


def get_es_connection(config: Config) -> Elasticsearch:

    es_client = Elasticsearch(
        hosts=config.es_hosts,
        basic_auth=(config.es_username, config.es_password),
        request_timeout=config.es_timeout,
        ca_certs=config.es_ssl_ca,
        verify_certs=config.es_verify_certs,
    )
    return es_client


def put_es_bulk(config: Config, actions, process_id, chunk_id) -> None:
    try:
        es_client = get_es_connection(config)
        helpers.bulk(client=es_client, actions=actions)
        logging.info(f"{process_id}: Chunk {chunk_id + 1} Pushed Successfully!")
    except Exception as e:
        logging.error(f"{process_id}: Error Pushing Chunk {chunk_id + 1}: {e}")
