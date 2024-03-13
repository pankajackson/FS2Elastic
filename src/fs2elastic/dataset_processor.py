import os, string, re
import pandas as pd
from typing import Any, Hashable
import logging
from datetime import datetime
from fs2elastic.es_handler import get_es_connection
from elasticsearch import helpers


class DatasetProcessor:
    def __init__(self, source_file, config):
        self.source_file = source_file
        self.config = config
        self.meta = {
            "created_at": datetime.utcfromtimestamp(os.path.getctime(source_file)),
            "modified_at": datetime.utcfromtimestamp(os.path.getmtime(source_file)),
            "source_path": source_file,
            "index": f"fs2elastic-{str(re.sub('['+re.escape(string.punctuation)+']', '',source_file)).replace(' ', '')}",
        }

    def df(self) -> pd.DataFrame:
        """Returns a pandas DataFrame from the source file."""
        df = pd.read_csv(self.source_file)
        df.fillna("", inplace=True)
        df["record_id"] = df.index
        return df

    def record_list(self) -> list[dict[Hashable, Any]]:
        """Converts the dataframe to a dictionary and returns it."""
        return self.df().to_dict(orient="records")

    def __generate_chunks(self, chunk_size):
        """The function `__generate_chunks` takes a chunk size as input and yields chunks of records from a list in that size."""
        for i in range(0, len(self.record_list()), chunk_size):
            yield self.record_list()[i : i + chunk_size]

    def record_list_chunks(self, chunk_size: int) -> list[list[dict[Hashable, Any]]]:
        """The function `record_list_chunks` returns a list of chunks generated from a given chunk size."""
        return list(self.__generate_chunks(chunk_size))

    def record_to_es_bulk_action(self, record, chunk_id, chunk_size):

        return {
            "_index": self.meta["index"],
            "_id": (chunk_id * chunk_size) + record["record_id"],
            "_source": {
                "record": record,
                "fs2e_meta": self.meta,
                "timestamp": datetime.utcnow(),
            },
        }

    def es_sync(self, chunk_size: int):
        """Synchronizes data with Elasticsearch using the configuration provided."""
        actions = []

        # Iterate over each chunk of records and send them to ES
        for chunk_id, chunk in enumerate(
            self.record_list_chunks(chunk_size=chunk_size)
        ):
            logging.info(f"Processing Chunk {chunk_id + 1}")
            if not chunk:  # If there are no more records, break out of loop
                break
            else:  # Otherwise, index the records into ES
                for record in chunk:
                    actions.append(
                        self.record_to_es_bulk_action(record, chunk_id, chunk_size)
                    )
                try:
                    logging.info(f"Pushing Chunk {chunk_id + 1}")
                    # Get an ES connection object
                    es_client = get_es_connection(self.config)
                    helpers.bulk(es_client, actions)
                except Exception as e:
                    logging.error(f"Error Pushing Chunk {chunk_id + 1}: {e}")
                logging.info(f"Pushed Chunk {chunk_id + 1}")
                logging.info(f"Cleaning Actions of Chunk {chunk_id + 1}")
                actions.clear()
