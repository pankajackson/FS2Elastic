import os, string, re, math
import pandas as pd
from typing import Any, Generator
from threading import current_thread
import logging
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from fs2elastic.es_handler import put_es_bulk
from fs2elastic.typings import Config


class DatasetProcessor:
    def __init__(self, source_file: str, config: Config, event_id: str) -> None:
        """
        The function initializes an object with source file information, configuration settings, and event
        ID, along with metadata including creation and modification timestamps and index details.

        :param source_file: The `source_file` parameter in the `__init__` method is a string that represents
        the path to a source file. It is used to initialize the `source_file` attribute of the class
        instance
        :type source_file: str
        :param config: The `config` parameter in the `__init__` method is of type `Config`. It is used to
        store configuration settings or options that can be accessed and used within the class. This could
        include settings related to the behavior of the class, default values, or any other configuration
        data needed for
        :type config: Config
        :param event_id: The `event_id` parameter in the `__init__` method is a string that represents the
        unique identifier for an event. It is one of the parameters required for initializing an instance of
        the class that this method belongs to
        :type event_id: str
        """
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
        """
        This function reads a file into a pandas DataFrame based on its file extension and performs some
        data cleaning operations.
        """
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

    def record_to_es_bulk_action(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        The function `record_to_es_bulk_action` creates a dictionary for bulk indexing a record in
        Elasticsearch.

        :param record: The `record` parameter is a dictionary containing information about a record. It is
        expected to have a key "record_id" which will be used as the "_id" in the Elasticsearch bulk action.
        The function `record_to_es_bulk_action` creates a dictionary in the format required for bulk
        indexing in
        :type record: dict[str, Any]
        :return: A dictionary is being returned with the following keys and values:
        - "_index": the value of self.meta["index"]
        - "_id": the value of record["record_id"]
        - "_source": a dictionary containing the following key-value pairs:
            - "record": the input record dictionary
            - "fs2e_meta": the value of self.meta
            - "timestamp": the current
        """
        return {
            "_index": self.meta["index"],
            "_id": record["record_id"],
            "_source": {
                "record": record,
                "fs2e_meta": self.meta,
                "timestamp": datetime.now(tz=pytz.UTC),
            },
        }

    def __generate_batches(
        self, max_batch_count: int
    ) -> Generator[pd.DataFrame, Any, None]:
        """
        This function generates batches of data from a DataFrame based on specified parameters.

        :param max_batch_count: The `max_batch_count` parameter specifies the maximum number of batches that
        should be generated during the batching process. This parameter is used to limit the number of
        batches generated based on a certain criteria or constraint
        :type max_batch_count: int
        """
        df_length = self.df().shape[0]
        batch_count = math.ceil(
            df_length
            / (self.config.dataset_chunk_size * self.config.dataset_threads_per_worker)
        )
        if batch_count > max_batch_count:
            batch_count = max_batch_count
        logging.info(
            f"{self.event_id}: Dataset processing proceeding in {batch_count} batch(es) "
        )
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
        """
        This function generates chunks of a pandas DataFrame based on a specified chunk size.

        :param data_frame: The `data_frame` parameter is a Pandas DataFrame that contains the data to be
        processed in chunks. The function `__generate_chunks` generates chunks of the DataFrame based on the
        specified chunk size from the configuration (`self.config.dataset_chunk_size`). It yields these
        chunks one by one using a generator
        :type data_frame: pd.DataFrame
        """
        for i in range(
            0,
            data_frame.shape[0],
            self.config.dataset_chunk_size,
        ):
            yield data_frame[i : i + self.config.dataset_chunk_size]

    def process_chunk(self, chunk: pd.DataFrame) -> None:
        """
        The function processes a chunk of data by converting it to Elasticsearch bulk actions and pushing it
        to Elasticsearch, with error handling.

        :param chunk: The `chunk` parameter in the `process_chunk` method is expected to be a pandas
        DataFrame containing the data that needs to be processed. This method processes the chunk of data by
        converting it into a dictionary format and then pushing it to Elasticsearch in bulk using the
        `put_es_bulk` function. If
        :type chunk: pd.DataFrame
        """
        try:
            put_es_bulk(
                config=self.config,
                actions=map(
                    self.record_to_es_bulk_action, chunk.to_dict(orient="records")
                ),
            )
        except Exception as e:
            logging.error(
                f"{self.event_id}: Error Pushing Chunk {current_thread().name}: {e}"
            )

    def process_batch(self, data_frame_batch: pd.DataFrame, batch_id: int) -> None:
        """
        The `process_batch` function processes a batch of data frames in parallel using ThreadPoolExecutor.

        :param data_frame_batch: The `data_frame_batch` parameter is a Pandas DataFrame that contains the
        data to be processed in batches
        :type data_frame_batch: pd.DataFrame
        :param batch_id: The `batch_id` parameter in the `process_batch` method is an integer that
        represents the identifier of the current batch being processed. It is used to uniquely identify the
        batch and can be helpful for tracking and logging purposes during batch processing
        :type batch_id: int
        """
        with ThreadPoolExecutor(
            max_workers=self.config.dataset_threads_per_worker,
            thread_name_prefix=f"{os.getpid()}:{batch_id}",
        ) as executor:
            for chunk_id, chunk in enumerate(self.__generate_chunks(data_frame_batch)):
                if chunk.empty:
                    break
                else:
                    try:
                        executor.submit(self.process_chunk, chunk)
                    except Exception as e:
                        logging.error(
                            f"{self.event_id}: Error Requesting Chunk {chunk_id + 1}: {e}"
                        )

    def process_dataframe(self):
        """
        The `process_dataframe` function uses a `ProcessPoolExecutor` to process batches of data in
        parallel.
        """
        with ProcessPoolExecutor(
            max_workers=self.config.dataset_max_workers
        ) as executor:
            for batch_id, batch in enumerate(
                self.__generate_batches(max_batch_count=self.config.dataset_max_workers)
            ):
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
        """
        The `es_sync` function in Python likely processes a dataframe.
        """
        self.process_dataframe()
