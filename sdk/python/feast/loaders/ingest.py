import logging
import os
from functools import partial
from multiprocessing import Pool
from typing import Iterable, List

import pandas as pd
import pyarrow.parquet as pq
from feast.constants import DATETIME_COLUMN
from feast.feature_set import FeatureSet
from feast.pyarrow_type_map import pa_column_to_timestamp_proto_column, \
    pa_column_to_proto_column
from feast.types import Field_pb2 as FieldProto
from feast.types.FeatureRow_pb2 import FeatureRow

_logger = logging.getLogger(__name__)

GRPC_CONNECTION_TIMEOUT_DEFAULT = 3  # type: int
GRPC_CONNECTION_TIMEOUT_APPLY = 300  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str
BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS = 300
CPU_COUNT = os.cpu_count()  # type: int
KAFKA_CHUNK_PRODUCTION_TIMEOUT = 120  # type: int


def _encode_pa_tables(
        file_dir: List[str],
        fs: FeatureSet,
) -> List[FeatureRow]:
    """
    Helper function to encode a PyArrow table(s) read from parquet file(s) into
    FeatureRows.

    This function accepts a list of file directory pointing to many parquet
    files. All parquet files must have the same schema.

    Each parquet file will be read into as a table and encoded into FeatureRows
    using a pool of max_workers workers.

    Args:
        file_dir (typing.List[str]):
            File directory of all the parquet files to encode.
            All parquet files must have the same schema.

        fs (feast.feature_set.FeatureSet):
            FeatureSet describing parquet files.

    Returns:
        List[FeatureRow]:
            List of FeatureRows encoded from the parquet file.
    """
    # Read parquet file as a PyArrow table
    table = pq.read_table(file_dir)

    # Add datetime column
    datetime_col = pa_column_to_timestamp_proto_column(
        table.column(DATETIME_COLUMN))

    # Preprocess the columns by converting all its values to Proto values
    proto_columns = {
        field_name: pa_column_to_proto_column(field.dtype,
                                              table.column(field_name))
        for field_name, field in fs.fields.items()
    }

    feature_set = f"{fs.name}:{fs.version}"

    # List to store result
    feature_rows = []

    # Loop optimization declaration
    field = FieldProto.Field
    proto_items = proto_columns.items()
    append = feature_rows.append

    # Iterate through the rows
    for row_idx in range(table.num_rows):
        feature_row = FeatureRow(event_timestamp=datetime_col[row_idx],
                                 feature_set=feature_set)
        # Loop optimization declaration
        ext = feature_row.fields.extend

        # Insert field from each column
        for k, v in proto_items:
            ext([field(name=k, value=v[row_idx])])

        append(feature_row)

    return feature_rows


def get_feature_row_chunks(
        files: List[str],
        fs: FeatureSet,
        max_workers: int
) -> Iterable[List[FeatureRow]]:
    """
    Iterator function to encode a PyArrow table read from a parquet file to
    FeatureRow(s).

    Args:
        files (typing.List[str]):
            File directory of all the parquet files to encode.
            All parquet files must have the same schema.

        fs (feast.feature_set.FeatureSet):
            FeatureSet describing parquet files.

        max_workers (int):
            Maximum number of workers to spawn.

    Returns:
        Iterable[List[FeatureRow]]:
            Iterable list of FeatureRow(s).
    """
    pool = Pool(max_workers)
    func = partial(_encode_pa_tables, fs=fs)
    for chunk in pool.imap_unordered(func, files):
        yield chunk
    return


def validate_dataframe(dataframe: pd.DataFrame, fs: FeatureSet):
    if "datetime" not in dataframe.columns:
        raise ValueError(
            f'Dataframe does not contain entity "datetime" in columns {dataframe.columns}'
        )

    for entity in fs.entities:
        if entity.name not in dataframe.columns:
            raise ValueError(
                f"Dataframe does not contain entity {entity.name} in columns {dataframe.columns}"
            )

    for feature in fs.features:
        if feature.name not in dataframe.columns:
            raise ValueError(
                f"Dataframe does not contain feature {feature.name} in columns {dataframe.columns}"
            )
