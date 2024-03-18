import os, string, re
import pandas as pd
from typing import Any, Generator, Hashable
import logging
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor
from elasticsearch import helpers
from fs2elastic.es_handler import get_es_connection, put_es_bulk
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
            "index": f"{self.config.es_index_prefix}{str(re.sub('['+re.escape(string.punctuation)+']', '',source_file)).replace(' ', '')}".lower(),
        }

    def df(self) -> pd.DataFrame:
        """Returns a pandas DataFrame from the source file."""
        match os.path.splitext(self.source_file)[-1]:
            case ".csv":
                df = pd.read_csv(self.source_file)
            case ".xlsx" | ".xls":
                df = pd.read_excel(self.source_file)
            case ".json":
                df = pd.read_json(self.source_file)
            case _:
                logging.error(
                    f"{self.process_id}: {os.path.splitext(self.source_file)[-1]} filetype not supported"
                )
                return pd.DataFrame()
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

        with ThreadPoolExecutor(
            max_workers=self.config.es_max_worker_count
        ) as executer:

            # Iterate over each chunk of records and send them to ES
            for chunk_id, chunk in enumerate(self.__generate_chunks()):
                if not chunk:  # If there are no more records, break out of loop
                    break
                else:  # Otherwise, index the records into ES
                    try:
                        executer.submit(
                            put_es_bulk,
                            self.config,
                            map(
                                self.record_to_es_bulk_action,
                                chunk,
                                [chunk_id] * len(chunk),
                            ),
                            self.process_id,
                            chunk_id,
                        )
                        logging.info(
                            f"{self.process_id}: Chunk {chunk_id + 1} Requested!"
                        )
                    except Exception as e:
                        logging.error(
                            f"{self.process_id}: Error Requesting Chunk {chunk_id + 1}: {e}"
                        )
