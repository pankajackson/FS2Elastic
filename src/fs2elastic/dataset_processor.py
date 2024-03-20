import os, string, re
import pandas as pd
from typing import Any, Generator
import logging
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from fs2elastic.es_handler import put_es_bulk
from fs2elastic.typings import Config


class DatasetProcessor:
    def __init__(self, source_file: str, config: Config, event_id: str) -> None:
        self.source_file = source_file
        self.config = config
        self.event_id = event_id
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
                    f"{self.event_id}: {os.path.splitext(self.source_file)[-1]} filetype not supported"
                )
                return pd.DataFrame()
        df.columns = df.columns.str.strip()
        df.fillna("", inplace=True)
        df["record_id"] = df.index
        return df

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

    def __generate_batches(
        self, batch_count: int
    ) -> Generator[pd.DataFrame, Any, None]:
        df_length = self.df().shape[0]
        print("Length is", "==" * 5, df_length)
        batch_size = df_length // batch_count
        extra_records = df_length % batch_count

        for i in range(batch_count):
            yield self.df()[
                i * batch_size
                + min(i, extra_records) : (i + 1) * batch_size
                + min(i + 1, extra_records)
            ]

    def __generate_chunks(
        self, data_frame: pd.DataFrame
    ) -> Generator[pd.DataFrame, Any, None]:
        for i in range(
            0,
            data_frame.shape[0],
            self.config.es_max_dataset_chunk_size,
        ):
            yield data_frame[i : i + self.config.es_max_dataset_chunk_size]

    def process_chunk(self, chunk: pd.DataFrame, chunk_id: int) -> None:
        put_es_bulk(
            config=self.config,
            actions=map(
                self.record_to_es_bulk_action,
                chunk.to_dict(orient="records"),
                [chunk_id] * len(chunk),
            ),
            event_id=self.event_id,
        )

    def process_batch(self, data_frame_batch: pd.DataFrame, batch_id: int) -> None:
        with ThreadPoolExecutor(
            max_workers=self.config.es_max_worker_count,
            thread_name_prefix=f"{os.getpid()}:{batch_id}",
        ) as executor:
            for chunk_id, chunk in enumerate(self.__generate_chunks(data_frame_batch)):
                if chunk.empty:
                    break
                else:
                    try:
                        executor.submit(self.process_chunk, chunk, chunk_id)
                    except Exception as e:
                        logging.error(
                            f"{self.event_id}: Error Requesting Chunk {chunk_id + 1}: {e}"
                        )

    def process_dataframe(self):
        with ProcessPoolExecutor(max_workers=8) as executor:
            for batch_id, batch in enumerate(self.__generate_batches(batch_count=8)):
                if batch.empty:
                    break
                else:
                    try:
                        executor.submit(self.process_batch, batch, batch_id)
                    except Exception as e:
                        logging.error(
                            f"{self.event_id}: Error Requesting Batch {batch_id + 1}: {e}"
                        )

    def es_sync(self):
        self.process_dataframe()
