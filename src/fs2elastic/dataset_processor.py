import os, time, string, re, datetime
import pandas as pd
from typing import Any, Hashable
from fs2elastic.es_handler import get_es_connection
from elasticsearch import helpers


class DatasetProcessor:
    def __init__(self, source_file, config):
        self.source_file = source_file
        self.config = config
        self.source_meta = {
            "created_at_epoch": os.path.getctime(source_file),
            "modified_at_epoch": os.path.getmtime(source_file),
            "created_at": time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.strptime(time.ctime(os.path.getctime(source_file))),
            ),
            "modified_at": time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.strptime(time.ctime(os.path.getctime(source_file))),
            ),
            # "modified_at_2": time.strptime(time.ctime(os.path.getctime(source_file))),
            # "modified_at_3": datetime.datetime.strptime(
            #     time.strftime(
            #         "%Y-%m-%d %H:%M:%S",
            #         time.strptime(time.ctime(os.path.getctime(source_file))),
            #     ),
            #     "%Y-%m-%d %H:%M:%S",
            # ),
        }
        chars = re.escape(string.punctuation)
        raw_index = source_file + "-" + str(os.path.getctime(source_file))
        self.es_meta = {
            "index": f"fs2elastic-{str(re.sub('['+chars+']', '',raw_index)).replace(' ', '')}"
        }

    def df(self) -> pd.DataFrame:
        """Returns a pandas DataFrame from the source file."""
        df = pd.read_csv(self.source_file)
        df["record_id"] = df.index
        df["index"] = self.es_meta["index"]
        for meta in self.source_meta.keys():
            df[meta] = self.source_meta[meta]
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

    def record_to_es_bulk_action(self, record, chunk, chunk_size):

        return {
            "_index": record["index"],
            "_id": (chunk * chunk_size) + record["record_id"],
            "_source": record,
        }

    def es_sync(self, chunk_size: int):
        """Synchronizes data with Elasticsearch using the configuration provided."""
        actions = []

        # Iterate over each chunk of records and send them to ES
        for chunk, records in enumerate(self.record_list_chunks(chunk_size=chunk_size)):
            if not records:  # If there are no more records, break out of loop
                break
            else:  # Otherwise, index the records into ES
                for record in records:
                    actions.append(
                        self.record_to_es_bulk_action(record, chunk, chunk_size)
                    )
                try:
                    # Get an ES connection object
                    es_client = get_es_connection(self.config)
                    helpers.bulk(es_client, actions)
                except Exception as e:
                    print(e)
                actions.clear()
