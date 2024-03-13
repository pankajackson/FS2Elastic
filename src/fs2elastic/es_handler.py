from elasticsearch import Elasticsearch


def get_es_connection(config) -> Elasticsearch:
    es_client = Elasticsearch(
        hosts=config["es_hosts"],
        basic_auth=(config["es_username"], config["es_password"]),
        request_timeout=300,
        ca_certs=config["es_ssl_ca"],
        verify_certs=config["es_verify_certs"],
    )
    return es_client
