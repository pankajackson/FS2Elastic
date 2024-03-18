import os, string, re
import pandas as pd
from typing import Any, Generator, Hashable
import logging
from datetime import datetime
import pytz
from elasticsearch import helpers
from fs2elastic.es_handler import get_es_connection
from fs2elastic.typings import Config


class DatasetProcessor:
    def __init__(self, source_file: str, config: Config, id: str) -> None:
        self.source_file = source_file
        self.config = config
        self.process_id = id
        self.meta = {
            "created_at": datetime.fromtimestamp(
                os.path.getctime(source_file), tz=pytz.UTC
            ),
            "modified_at": datetime.fromtimestamp(
                os.path.getmtime(source_file), tz=pytz.UTC
            ),
            "source_path": source_file,
            "index": f"fs2elastic-{str(re.sub('['+re.escape(string.punctuation)+']', '',source_file)).replace(' ', '')}".lower(),
        }

    def df(self) -> pd.DataFrame:
        """Returns a pandas DataFrame from the source file."""
        df = pd.read_csv(self.source_file)
        df.columns = df.columns.str.strip()
        df.fillna("", inplace=True)
        df["record_id"] = df.index
        return df

    def record_list(self) -> list[dict[Hashable, Any]]:
        """Converts the dataframe to a dictionary and returns it."""
        return self.df().to_dict(orient="records")

    def __generate_chunks(self) -> Generator[list[dict[Hashable, Any]], Any, None]:
        """The function `__generate_chunks` takes a chunk size as input and yields chunks of records from a list in that size."""
        for i in range(
            0, len(self.record_list()), self.config.es_max_dataset_chunk_size
        ):
            logging.info(
                "%s: Generating Chunk of %s from index %s"
                % (self.process_id, self.config.es_max_dataset_chunk_size, i)
            )
            yield self.record_list()[i : i + self.config.es_max_dataset_chunk_size]

    def record_to_es_bulk_action(
        self, record: dict[str, Any], chunk_id: int
    ) -> dict[str, Any]:

        return {
            "_index": self.meta["index"],
            "_id": (chunk_id * self.config.es_max_dataset_chunk_size)
            + record["record_id"],
            "_source": {
                "record": record,
                "fs2e_meta": self.meta,
                "timestamp": datetime.now(tz=pytz.UTC),
            },
        }

    def es_sync(self) -> None:
        """Synchronizes data with Elasticsearch using the configuration provided."""

        es_client = get_es_connection(self.config)

        # Iterate over each chunk of records and send them to ES
        for chunk_id, chunk in enumerate(self.__generate_chunks()):
            if not chunk:  # If there are no more records, break out of loop
                break
            else:  # Otherwise, index the records into ES
                try:
                    helpers.bulk(
                        client=es_client,
                        actions=map(
                            self.record_to_es_bulk_action,
                            chunk,
                            [chunk_id] * len(chunk),
                        ),
                    )
                    logging.info(
                        f"{self.process_id}: Chunk {chunk_id + 1} Pushed Successfully!"
                    )
                except Exception as e:
                    logging.error(
                        f"{self.process_id}: Error Pushing Chunk {chunk_id + 1}: {e}"
                    )
